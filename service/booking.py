import asyncio
import asyncpg
import grpc
import uuid
import json
import aiokafka
from datetime import datetime
import logging
from circuitbreaker import circuit

import booking_pb2
import booking_pb2_grpc
import user_pb2
import user_pb2_grpc
import schedule_pb2
import schedule_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BookingService')

class BookingService(booking_pb2_grpc.BookingServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        self.kafka_producer = None
        self.user_service_address = 'user-service:50052'
        self.schedule_service_address = 'schedule-service:50051'
        
    async def pg_connect(self):
        for _ in range(5):
            try:
                self.pg_conn = await asyncpg.connect(
                    host='postgres-booking',
                    database='booking_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to booking database")
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"Database connection error, attempt {_}: {e}")
                await asyncio.sleep(5)
    async def setup_tables(self):
        """Создание таблиц в PostgreSQL"""
        try:
            # Таблица бронирований
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS bookings (
                    booking_id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    workout_id VARCHAR(36) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Вставляем тестовые данные
            await self.pg_conn.execute("""
                INSERT INTO bookings (booking_id, user_id, workout_id, status, created_at) VALUES
                ('booking-001', '550e8400-e29b-41d4-a716-446655440001', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '2 days'),
                ('booking-002', '550e8400-e29b-41d4-a716-446655440002', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '1 day'),
                ('booking-003', '550e8400-e29b-41d4-a716-446655440003', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '3 days'),
                ('booking-004', '550e8400-e29b-41d4-a716-446655440001', 'workout-002', 'CONFIRMED', CURRENT_DATE - INTERVAL '5 days'),
                ('booking-005', '550e8400-e29b-41d4-a716-446655440005', 'workout-002', 'CONFIRMED', CURRENT_DATE - INTERVAL '4 days'),
                ('booking-006', '550e8400-e29b-41d4-a716-446655440003', 'workout-003', 'CONFIRMED', CURRENT_DATE - INTERVAL '2 days'),
                ('booking-007', '550e8400-e29b-41d4-a716-446655440002', 'workout-004', 'CONFIRMED', CURRENT_DATE - INTERVAL '1 day'),
                ('booking-008', '550e8400-e29b-41d4-a716-446655440001', 'workout-005', 'CONFIRMED', CURRENT_DATE),
                ('booking-009', '550e8400-e29b-41d4-a716-446655440004', 'workout-001', 'CANCELLED', CURRENT_DATE - INTERVAL '3 days'),
                ('booking-010', '550e8400-e29b-41d4-a716-446655440002', 'workout-008', 'CANCELLED', CURRENT_DATE - INTERVAL '2 days'),
                ('booking-011', '550e8400-e29b-41d4-a716-446655440005', 'workout-006', 'PENDING', CURRENT_DATE - INTERVAL '1 day'),
                ('booking-012', '550e8400-e29b-41d4-a716-446655440003', 'workout-007', 'PENDING', CURRENT_DATE)
                ON CONFLICT (booking_id) DO NOTHING
            """)
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    async def kafka_connect(self):
        try:
            self.kafka_producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers='kafka:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            await self.kafka_producer.start()
            logger.info("Successfully connected to Kafka")
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
    
    async def CreateBooking(self, request, context):
        try:
            # Start SAGA: Step 1 - Validate User Subscription
            validation = await self.UserValidated(
                booking_pb2.UserValidationRequest(
                    user_id=request.user_id,
                    workout_id=request.workout_id
                ),
                context
            )
            
            if not validation.valid:
                await self.publish_booking_cancelled(request.user_id, request.workout_id, validation.message)
                return booking_pb2.BookingResponse()
            
            # Step 2 - Reserve slot in Schedule Service
            async with grpc.aio.insecure_channel(self.schedule_service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                reserve_response = await stub.ReserveSlot(
                    schedule_pb2.ReserveSlotRequest(
                        workout_id=request.workout_id
                    )
                )
            
            if not reserve_response.success:
                await self.publish_booking_cancelled(request.user_id, request.workout_id, reserve_response.message)
                return booking_pb2.BookingResponse()
            
            # Step 3 - Create booking record
            booking_id = str(uuid.uuid4())
            
            await self.pg_conn.execute(
                """
                INSERT INTO bookings 
                (booking_id, user_id, workout_id, status, created_at, updated_at)
                VALUES ($1, $2, $3, 'CONFIRMED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                booking_id,
                request.user_id,
                request.workout_id
            )
            
            # Publish BookingConfirmed event for Saga completion
            await self.publish_booking_confirmed(
                booking_id,
                request.user_id,
                request.workout_id
            )
            
            return await self.GetBooking(
                booking_pb2.GetBookingRequest(booking_id=booking_id),
                context
            )
            
        except Exception as e:
            logger.error(f'CreateBooking failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CreateBooking failed: {str(e)}')
            return booking_pb2.BookingResponse()
    
    @circuit(failure_threshold=5, recovery_timeout=30)
    async def UserValidated(self, request, context):
        """Circuit Breaker pattern for user validation"""
        try:
            async with grpc.aio.insecure_channel(self.user_service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.ValidateSubscription(
                    user_pb2.ValidateSubscriptionRequest(user_id=request.user_id)
                )
            
            if response.valid:
                return booking_pb2.UserValidationResponse(
                    valid=True,
                    message='User subscription is valid'
                )
            else:
                return booking_pb2.UserValidationResponse(
                    valid=False,
                    message=response.message
                )
                
        except grpc.RpcError as e:
            logger.warning(f'User service unavailable: {e}')
            # Fallback: allow booking if user service is down (circuit breaker open)
            return booking_pb2.UserValidationResponse(
                valid=True,
                message='User service unavailable, proceeding with booking'
            )
        except Exception as e:
            logger.error(f'User validation failed: {str(e)}')
            return booking_pb2.UserValidationResponse(
                valid=False,
                message=f'User validation failed: {str(e)}'
            )
    
    async def CancelBooking(self, request, context):
        try:
            # Get booking details
            booking = await self.pg_conn.fetchrow(
                "SELECT * FROM bookings WHERE booking_id = $1",
                request.booking_id
            )
            
            if not booking:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Booking not found')
                return booking_pb2.BookingResponse()
            
            # Cancel slot reservation
            async with grpc.aio.insecure_channel(self.schedule_service_address) as channel:
                stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                await stub.CancelReservation(
                    schedule_pb2.CancelReservationRequest(
                        workout_id=booking['workout_id']
                    )
                )
            
            # Update booking status
            await self.pg_conn.execute(
                """
                UPDATE bookings 
                SET status = 'CANCELLED',
                    updated_at = CURRENT_TIMESTAMP
                WHERE booking_id = $1
                """,
                request.booking_id
            )
            
            # Publish cancellation event
            await self.publish_booking_cancelled(
                booking['user_id'],
                booking['workout_id'],
                'Booking cancelled by user'
            )
            
            return await self.GetBooking(request, context)
            
        except Exception as e:
            logger.error(f'CancelBooking failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CancelBooking failed: {str(e)}')
            return booking_pb2.BookingResponse()
    
    async def GetUserBookings(self, request, context):
        try:
            bookings = await self.pg_conn.fetch(
                "SELECT * FROM bookings WHERE user_id = $1 ORDER BY created_at DESC",
                request.user_id
            )
            
            response = booking_pb2.BookingsResponse()
            for booking in bookings:
                response.bookings.append(booking_pb2.BookingResponse(
                    booking_id=str(booking['booking_id']),
                    user_id=str(booking['user_id']),
                    workout_id=str(booking['workout_id']),
                    status=str(booking['status']),
                    created_at=str(booking['created_at']),
                    updated_at=str(booking['updated_at'])
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetUserBookings failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetUserBookings failed: {str(e)}')
            return booking_pb2.BookingsResponse()
    
    async def GetBooking(self, request, context):
        try:
            booking = await self.pg_conn.fetchrow(
                "SELECT * FROM bookings WHERE booking_id = $1",
                request.booking_id
            )
            
            if not booking:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Booking not found')
                return booking_pb2.BookingResponse()
            
            return booking_pb2.BookingResponse(
                booking_id=str(booking['booking_id']),
                user_id=str(booking['user_id']),
                workout_id=str(booking['workout_id']),
                status=str(booking['status']),
                created_at=str(booking['created_at']),
                updated_at=str(booking['updated_at'])
            )
        except Exception as e:
            logger.error(f'GetBooking failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetBooking failed: {str(e)}')
            return booking_pb2.BookingResponse()
    
    async def publish_booking_confirmed(self, booking_id, user_id, workout_id):
        """Publish event to Kafka for Saga completion"""
        try:
            event = {
                'event_type': 'BookingConfirmed',
                'booking_id': booking_id,
                'user_id': user_id,
                'workout_id': workout_id,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.kafka_producer.send_and_wait(
                'booking-events',
                event
            )
            logger.info(f'Published BookingConfirmed event: {event}')
        except Exception as e:
            logger.error(f'Failed to publish booking confirmed event: {e}')
    
    async def publish_booking_cancelled(self, user_id, workout_id, reason):
        """Publish event to Kafka for Saga rollback"""
        try:
            event = {
                'event_type': 'BookingCancelled',
                'user_id': user_id,
                'workout_id': workout_id,
                'reason': reason,
                'timestamp': datetime.now().isoformat()
            }
            
            await self.kafka_producer.send_and_wait(
                'booking-events',
                event
            )
            logger.info(f'Published BookingCancelled event: {event}')
        except Exception as e:
            logger.error(f'Failed to publish booking cancelled event: {e}')

async def serve():
    server = grpc.aio.server()
    service = BookingService()
    await service.pg_connect()
    await service.kafka_connect()
    booking_pb2_grpc.add_BookingServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50053')
    
    await server.start()
    logger.info("Booking Service started on port 50053")
    
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Booking Service stopped by user")
    except Exception as e:
        logger.error(f"Booking Service crashed: {str(e)}")