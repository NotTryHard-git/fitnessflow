import asyncio
import asyncpg
import grpc
import json
import aiokafka
import uuid
import logging
import time
import notification_pb2
import notification_pb2_grpc
from datetime import datetime
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('NotificationService')

class NotificationService(notification_pb2_grpc.NotificationServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        self.kafka_consumer = None
        
    async def pg_connect(self):
        for _ in range(5):
            try:
                self.pg_conn = await asyncpg.connect(
                    host='postgres-notification',
                    database='attendance_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to notification database")
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"Database connection error, attempt {_}: {e}")
                await asyncio.sleep(5)
    async def setup_tables(self):
        """Создание таблиц в PostgreSQL"""
        try:
            # Таблица уведомлений
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    notification_id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    type VARCHAR(50) NOT NULL,
                    title VARCHAR(255) NOT NULL,
                    message TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    related_booking_id VARCHAR(36),
                    related_workout_id VARCHAR(36)
                )
            """)
            
            # Вставляем тестовые данные для уведомлений
            await self.pg_conn.execute("""
                INSERT INTO notifications (notification_id, user_id, type, title, message, related_booking_id, related_workout_id) VALUES
                ('notif-001', '550e8400-e29b-41d4-a716-446655440001', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Утренняя йога"', 'booking-001', 'workout-001'),
                ('notif-002', '550e8400-e29b-41d4-a716-446655440002', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Утренняя йога"', 'booking-002', 'workout-001'),
                ('notif-003', '550e8400-e29b-41d4-a716-446655440003', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Силовая тренировка"', 'booking-004', 'workout-002'),
                ('notif-004', '550e8400-e29b-41d4-a716-446655440005', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Аквааэробика"', 'booking-008', 'workout-005'),
                ('notif-005', '550e8400-e29b-41d4-a716-446655440004', 'CANCELLED_BOOKING', 'Бронирование отменено', 'Ваше бронирование на тренировку "Утренняя йога" было отменено', 'booking-009', 'workout-001'),
                ('notif-006', '550e8400-e29b-41d4-a716-446655440002', 'CANCELLED_BOOKING', 'Тренировка отменена', 'Тренировка "Вечерний стретчинг" была отменена администратором', 'booking-010', 'workout-008')
                ON CONFLICT (notification_id) DO NOTHING
            """)
            
            
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    async def kafka_connect(self):
        try:
            self.kafka_consumer = aiokafka.AIOKafkaConsumer(
                'booking-events',
                bootstrap_servers='kafka:9092',
                group_id='notification-service',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            await self.kafka_consumer.start()
            logger.info("Successfully connected to Kafka")
            
            # Start consuming messages in background
            asyncio.create_task(self.consume_messages())
            
        except Exception as e:
            logger.error(f"Kafka connection error: {e}")
    
    async def consume_messages(self):
        """Consume messages from Kafka and create notifications"""
        try:
            async for message in self.kafka_consumer:
                event = message.value
                logger.info(f"Received event: {event}")
                
                if event['event_type'] == 'BookingConfirmed':
                    await self.create_success_notification(
                        event['user_id'],
                        event['booking_id'],
                        event['workout_id']
                    )
                elif event['event_type'] == 'BookingCancelled':
                    await self.create_cancellation_notification(
                        event['user_id'],
                        event['workout_id'],
                        event.get('reason', 'Unknown reason')
                    )
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
    
    async def create_success_notification(self, user_id, booking_id, workout_id):
        """Create notification for successful booking"""
        try:
            notification_id = str(uuid.uuid4())
            
            await self.pg_conn.execute(
                """
                INSERT INTO notifications 
                (notification_id, user_id, type, title, message, related_booking_id, related_workout_id)
                VALUES ($1, $2, 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 
                        'Вы успешно записались на тренировку', $3, $4)
                """,
                notification_id,
                user_id,
                booking_id,
                workout_id
            )
            
            logger.info(f"Created success notification for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to create success notification: {e}")
    
    async def create_cancellation_notification(self, user_id, workout_id, reason):
        """Create notification for booking cancellation"""
        try:
            notification_id = str(uuid.uuid4())
            
            await self.pg_conn.execute(
                """
                INSERT INTO notifications 
                (notification_id, user_id, type, title, message, related_workout_id)
                VALUES ($1, $2, 'CANCELLED_BOOKING', 'Бронирование отменено', 
                        $3, $4)
                """,
                notification_id,
                user_id,
                reason,
                workout_id
            )
            
            logger.info(f"Created cancellation notification for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to create cancellation notification: {e}")
    
    async def SendNotification(self, request, context):
        """Direct notification sending"""
        try:
            notification_id = str(uuid.uuid4())
            
            await self.pg_conn.execute(
                """
                INSERT INTO notifications 
                (notification_id, user_id, type, title, message, related_booking_id, related_workout_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                notification_id,
                request.user_id,
                request.type,
                request.title,
                request.message,
                request.related_booking_id,
                request.related_workout_id
            )
            
            return notification_pb2.NotificationResponse(
                notification_id=notification_id,
                user_id=request.user_id,
                type=request.type,
                title=request.title,
                message=request.message,
                created_at=datetime.now().isoformat(),
                related_booking_id=request.related_booking_id,
                related_workout_id=request.related_workout_id
            )
        except Exception as e:
            logger.error(f'SendNotification failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'SendNotification failed: {str(e)}')
            return notification_pb2.NotificationResponse()
    
    async def GetUserNotifications(self, request, context):
        try:
            query = """
                SELECT * FROM notifications 
                WHERE user_id = $1 
                ORDER BY created_at DESC
            """
            
            if request.limit > 0:
                query += f" LIMIT {request.limit}"
            
            notifications = await self.pg_conn.fetch(
                query,
                request.user_id
            )
            
            response = notification_pb2.NotificationsResponse()
            for notification in notifications:
                response.notifications.append(notification_pb2.NotificationResponse(
                    notification_id=str(notification['notification_id']),
                    user_id=str(notification['user_id']),
                    type=str(notification['type']),
                    title=str(notification['title']),
                    message=str(notification['message']),
                    created_at=str(notification['created_at']),
                    related_booking_id=str(notification['related_booking_id']) if notification['related_booking_id'] else '',
                    related_workout_id=str(notification['related_workout_id']) if notification['related_workout_id'] else ''
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetUserNotifications failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetUserNotifications failed: {str(e)}')
            return notification_pb2.NotificationsResponse()

async def serve():
    server = grpc.aio.server()
    service = NotificationService()
    await service.pg_connect()
    time.sleep(30)
    await service.kafka_connect()
    notification_pb2_grpc.add_NotificationServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50055')
    
    await server.start()
    logger.info("Notification Service started on port 50055")
    
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Notification Service stopped by user")
    except Exception as e:
        logger.error(f"Notification Service crashed: {str(e)}")