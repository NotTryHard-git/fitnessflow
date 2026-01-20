import asyncio
import asyncpg
import grpc
import uuid
import json
import aiokafka
from datetime import datetime
import logging
from circuitbreaker import circuit, CircuitBreaker
from typing import Tuple

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

# Глобальный Circuit Breaker для User Service
user_service_circuit_breaker = CircuitBreaker(
    failure_threshold=5,           # Открывается после 5 ошибок
    recovery_timeout=30,           # Закрывается через 30 секунд
    name="UserServiceCircuitBreaker",
    expected_exception=grpc.RpcError  # Обрабатываем gRPC ошибки
)

class BookingService(booking_pb2_grpc.BookingServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        self.kafka_producer = None
        self.user_service_address = 'user-service:50052'
        self.schedule_service_address = 'schedule-service:50051'
        self.circuit_breaker_state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.circuit_breaker_failures = 0
        self.last_failure_time = None
        
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
    
    @circuit(failure_threshold=5, recovery_timeout=30)
    async def validate_user_with_circuit_breaker(self, user_id: str) -> Tuple[bool, str]:
        """
        Валидация пользователя с Circuit Breaker защитой
        
        Args:
            user_id: ID пользователя для валидации
            
        Returns:
            Tuple[bool, str]: (валиден ли пользователь, сообщение)
            
        Raises:
            Exception: Если Circuit Breaker открыт или произошла ошибка
        """
        try:
            logger.info(f"Validating user {user_id} through User Service...")
            
            async with grpc.aio.insecure_channel(self.user_service_address) as channel:
                stub = user_pb2_grpc.UserServiceStub(channel)
                response = await stub.ValidateSubscription(
                    user_pb2.ValidateSubscriptionRequest(user_id=user_id),
                    timeout=2.0
                )
            
            if response.valid:
                logger.info(f"User {user_id} validation successful")
                return True, response.message
            else:
                logger.warning(f"User {user_id} validation failed: {response.message}")
                return False, response.message
                
        except grpc.RpcError as e:
            logger.warning(f"User service gRPC error: {e.code()} - {e.details()}")
            raise  # Пробрасываем для Circuit Breaker
            
        except Exception as e:
            logger.error(f"Unexpected error in user validation: {e}")
            raise
    
    async def check_and_update_circuit_breaker(self, error_occurred: bool = False):
        """
        Проверка и обновление состояния Circuit Breaker
        
        Args:
            error_occurred: Произошла ли ошибка в текущем запросе
        """
        if error_occurred:
            self.circuit_breaker_failures += 1
            self.last_failure_time = datetime.now()
            
            if self.circuit_breaker_failures >= 5 and self.circuit_breaker_state != 'OPEN':
                self.circuit_breaker_state = 'OPEN'
                logger.warning(f"⚡ CIRCUIT BREAKER IS NOW OPEN! (failures: {self.circuit_breaker_failures})")
        else:
            # Сброс счетчика при успешном запросе
            if self.circuit_breaker_state != 'CLOSED':
                self.circuit_breaker_state = 'CLOSED'
                self.circuit_breaker_failures = 0
                logger.info("✓ CIRCUIT BREAKER IS NOW CLOSED")
    
    async def can_proceed_with_booking(self) -> Tuple[bool, str]:
        """
        Проверка, можно ли продолжить с бронированием (проверка Circuit Breaker)
        
        Returns:
            Tuple[bool, str]: (можно продолжать, причина)
        """
        if self.circuit_breaker_state == 'OPEN':
            # Проверяем, не истек ли таймаут восстановления
            if self.last_failure_time:
                time_since_failure = (datetime.now() - self.last_failure_time).seconds
                if time_since_failure >= 30:
                    # Переходим в HALF_OPEN для тестирования
                    self.circuit_breaker_state = 'HALF_OPEN'
                    logger.info("Circuit Breaker transitioned to HALF_OPEN state")
                    return True, "Testing in HALF_OPEN state"
            
            return False, "Circuit Breaker is OPEN - User service unavailable"
        
        return True, "Circuit Breaker is CLOSED - Proceeding normally"
    
    async def CreateBooking(self, request, context):
        """
        Создание бронирования с защитой Circuit Breaker
        
        Workflow:
        1. Проверка состояния Circuit Breaker
        2. Валидация пользователя (защищено Circuit Breaker)
        3. Резервирование слота в Schedule Service
        4. Создание записи бронирования
        5. Публикация события
        """
        logger.info(f"Creating booking for user {request.user_id}, workout {request.workout_id}")
        
        try:
            # Шаг 1: Проверка Circuit Breaker
            can_proceed, reason = await self.can_proceed_with_booking()
            if not can_proceed:
                logger.warning(f"CIRCUIT BREAKER BLOCKED: {reason}")
                
                await self.publish_booking_cancelled(
                    request.user_id,
                    request.workout_id,
                    f"Service unavailable: {reason}"
                )
                
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(f"CIRCUIT BREAKER: {reason}")
                return booking_pb2.BookingResponse()
            
            # Шаг 2: Валидация пользователя
            user_valid = False
            user_validation_message = ""
            
            try:
                user_valid, user_validation_message = await self.validate_user_with_circuit_breaker(request.user_id)
                await self.check_and_update_circuit_breaker(error_occurred=False)
                
            except Exception as e:
                await self.check_and_update_circuit_breaker(error_occurred=True)
                
                # Если мы в HALF_OPEN состоянии и получили ошибку, возвращаемся в OPEN
                if self.circuit_breaker_state == 'HALF_OPEN':
                    self.circuit_breaker_state = 'OPEN'
                    logger.warning("HALF_OPEN test failed, returning to OPEN state")
                
                logger.error(f"User validation failed with Circuit Breaker: {e}")
                
                # Fallback логика: разрешаем бронирование без проверки?
                # В реальной системе это бизнес-решение
                ALLOW_BOOKING_WITHOUT_VALIDATION = False
                
                if not ALLOW_BOOKING_WITHOUT_VALIDATION:
                    await self.publish_booking_cancelled(
                        request.user_id,
                        request.workout_id,
                        f"User service unavailable: {str(e)}"
                    )
                    
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details(f"User service unavailable: {str(e)}")
                    return booking_pb2.BookingResponse()
                else:
                    logger.warning("Using fallback: allowing booking without user validation")
                    user_valid = True
                    user_validation_message = "Fallback: User service unavailable"
            
            if not user_valid:
                await self.publish_booking_cancelled(
                    request.user_id,
                    request.workout_id,
                    f"User validation failed: {user_validation_message}"
                )
                
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(f"User validation failed: {user_validation_message}")
                return booking_pb2.BookingResponse()
            
            # Шаг 3: Резервирование слота в Schedule Service
            try:
                logger.info(f"Reserving slot for workout {request.workout_id}")
                
                async with grpc.aio.insecure_channel(self.schedule_service_address) as channel:
                    stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                    reserve_response = await stub.ReserveSlot(
                        schedule_pb2.ReserveSlotRequest(
                            workout_id=request.workout_id
                        ),
                        timeout=3.0
                    )
                
                if not reserve_response.success:
                    await self.publish_booking_cancelled(
                        request.user_id,
                        request.workout_id,
                        f"Slot reservation failed: {reserve_response.message}"
                    )
                    
                    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                    context.set_details(reserve_response.message)
                    return booking_pb2.BookingResponse()
                    
                logger.info(f"Slot reserved successfully. Current participants: {reserve_response.current_participants}")
                
            except grpc.RpcError as e:
                await self.publish_booking_cancelled(
                    request.user_id,
                    request.workout_id,
                    f"Schedule service error: {e.details()}"
                )
                
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                context.set_details(f"Schedule service error: {e.details()}")
                return booking_pb2.BookingResponse()
            
            # Шаг 4: Создание записи бронирования
            booking_id = str(uuid.uuid4())
            logger.info(f"Creating booking record: {booking_id}")
            
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
            
            # Шаг 5: Публикация события об успешном бронировании
            await self.publish_booking_confirmed(
                booking_id,
                request.user_id,
                request.workout_id
            )
            
            logger.info(f"✅ Booking {booking_id} created successfully for user {request.user_id}")
            
            return await self.GetBooking(
                booking_pb2.GetBookingRequest(booking_id=booking_id),
                context
            )
            
        except Exception as e:
            logger.error(f'CreateBooking failed: {str(e)}')
            
            # Всегда публикуем событие об отмене при ошибке
            await self.publish_booking_cancelled(
                request.user_id,
                request.workout_id,
                f"Booking creation failed: {str(e)}"
            )
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CreateBooking failed: {str(e)}')
            return booking_pb2.BookingResponse()
    
    async def CancelBooking(self, request, context):
        """Отмена бронирования"""
        try:
            logger.info(f"Cancelling booking: {request.booking_id}")
            
            # Получаем информацию о бронировании
            booking = await self.pg_conn.fetchrow(
                "SELECT * FROM bookings WHERE booking_id = $1",
                request.booking_id
            )
            
            if not booking:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Booking not found')
                return booking_pb2.BookingResponse()
            
            # Освобождаем слот в Schedule Service
            try:
                async with grpc.aio.insecure_channel(self.schedule_service_address) as channel:
                    stub = schedule_pb2_grpc.ScheduleServiceStub(channel)
                    await stub.CancelReservation(
                        schedule_pb2.CancelReservationRequest(
                            workout_id=booking['workout_id']
                        )
                    )
            except Exception as e:
                logger.warning(f"Failed to cancel reservation in Schedule Service: {e}")
                # Продолжаем даже если не удалось освободить слот
            
            # Обновляем статус бронирования
            await self.pg_conn.execute(
                """
                UPDATE bookings 
                SET status = 'CANCELLED',
                    updated_at = CURRENT_TIMESTAMP
                WHERE booking_id = $1
                """,
                request.booking_id
            )
            
            # Публикуем событие об отмене
            await self.publish_booking_cancelled(
                booking['user_id'],
                booking['workout_id'],
                'Booking cancelled by user'
            )
            
            logger.info(f"✅ Booking {request.booking_id} cancelled successfully")
            
            return await self.GetBooking(request, context)
            
        except Exception as e:
            logger.error(f'CancelBooking failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CancelBooking failed: {str(e)}')
            return booking_pb2.BookingResponse()
    
    async def GetUserBookings(self, request, context):
        """Получение бронирований пользователя"""
        try:
            logger.info(f"Getting bookings for user: {request.user_id}")
            
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
            
            logger.info(f"Found {len(bookings)} bookings for user {request.user_id}")
            return response
            
        except Exception as e:
            logger.error(f'GetUserBookings failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetUserBookings failed: {str(e)}')
            return booking_pb2.BookingsResponse()
    
    async def GetBooking(self, request, context):
        """Получение информации о бронировании"""
        try:
            logger.info(f"Getting booking: {request.booking_id}")
            
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
    
    async def UserValidated(self, request, context):
        """Валидация пользователя (используется в SAGA)"""
        try:
            logger.info(f"Validating user for SAGA: {request.user_id}")
            
            # Проверка Circuit Breaker
            can_proceed, reason = await self.can_proceed_with_booking()
            if not can_proceed:
                logger.warning(f"CIRCUIT BREAKER in UserValidated: {reason}")
                return booking_pb2.UserValidationResponse(
                    valid=False,
                    message=f"CIRCUIT BREAKER: {reason}"
                )
            
            try:
                is_valid, message = await self.validate_user_with_circuit_breaker(request.user_id)
                await self.check_and_update_circuit_breaker(error_occurred=False)
                
                return booking_pb2.UserValidationResponse(
                    valid=is_valid,
                    message=message
                )
                
            except Exception as e:
                await self.check_and_update_circuit_breaker(error_occurred=True)
                
                logger.error(f"User validation failed in UserValidated: {e}")
                return booking_pb2.UserValidationResponse(
                    valid=False,
                    message=f"Service unavailable: {str(e)}"
                )
            
        except Exception as e:
            logger.error(f'UserValidated failed: {str(e)}')
            return booking_pb2.UserValidationResponse(
                valid=False,
                message=f"Validation failed: {str(e)}"
            )
    
    async def publish_booking_confirmed(self, booking_id: str, user_id: str, workout_id: str):
        """Публикация события подтверждения бронирования"""
        try:
            event = {
                'event_type': 'BookingConfirmed',
                'booking_id': booking_id,
                'user_id': user_id,
                'workout_id': workout_id,
                'timestamp': datetime.now().isoformat(),
                'circuit_breaker_state': self.circuit_breaker_state
            }
            
            if self.kafka_producer:
                await self.kafka_producer.send_and_wait(
                    'booking-events',
                    event
                )
                logger.info(f"📤 Published BookingConfirmed event: {booking_id}")
            else:
                logger.warning("Kafka producer not available, event not published")
                
        except Exception as e:
            logger.error(f'Failed to publish booking confirmed event: {e}')
    
    async def publish_booking_cancelled(self, user_id: str, workout_id: str, reason: str):
        """Публикация события отмены бронирования"""
        try:
            event = {
                'event_type': 'BookingCancelled',
                'user_id': user_id,
                'workout_id': workout_id,
                'reason': reason,
                'timestamp': datetime.now().isoformat(),
                'circuit_breaker_state': self.circuit_breaker_state
            }
            
            if self.kafka_producer:
                await self.kafka_producer.send_and_wait(
                    'booking-events',
                    event
                )
                logger.info(f"📤 Published BookingCancelled event: {reason[:50]}...")
            else:
                logger.warning("Kafka producer not available, event not published")
                
        except Exception as e:
            logger.error(f'Failed to publish booking cancelled event: {e}')

async def serve():
    """Запуск gRPC сервера"""
    server = grpc.aio.server()
    service = BookingService()
    
    # Подключаемся к БД и Kafka
    await service.pg_connect()
    await service.kafka_connect()
    
    # Регистрируем сервис
    booking_pb2_grpc.add_BookingServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50053')
    
    # Запускаем сервер
    await server.start()
    logger.info("🚀 Booking Service started on port 50053")
    logger.info(f"Circuit Breaker state: {service.circuit_breaker_state}")
    
    # Основной цикл
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Booking Service stopped by user")
    except Exception as e:
        logger.error(f"Booking Service crashed: {str(e)}")