import grpc
import asyncio
import asyncpg as pg
import json
import uuid
from datetime import datetime
import logging
import os
import threading
import time
import queue
from kafka import KafkaProducer, KafkaConsumer
import attendance_pb2, attendance_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AttendanceService')

class AttendanceService(attendance_pb2_grpc.AttendanceServiceServicer):
    def __init__(self):
        logger.info('Initializing Attendance Service...')
        self.pg_conn = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.consumer_thread = None
        self.message_queue = queue.Queue()
        self.running = False
        
        # Инициализируем только Producer сразу
        self.init_kafka_producer()
    
    def init_kafka_producer(self):
        """Инициализация только Kafka producer"""
        try:
            logger.info("Initializing Kafka producer...")
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 5, 0),
                acks='all',
                retries=3,
                max_block_ms=5000
            )
            logger.info("Kafka producer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            logger.warning("Kafka producer will not be available")
            self.kafka_producer = None
    
    def start_kafka_consumer_thread(self):
        """Запуск Kafka Consumer в отдельном потоке"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            return
        
        logger.info("Starting Kafka consumer thread...")
        self.running = True
        self.consumer_thread = threading.Thread(
            target=self.kafka_consumer_worker,
            daemon=True
        )
        self.consumer_thread.start()
    
    def kafka_consumer_worker(self):
        """Рабочая функция для потока Consumer"""
        logger.info("Kafka consumer worker started")
        
        try:
            # Инициализируем Consumer в этом потоке
            consumer = KafkaConsumer(
                'notification-created',
                bootstrap_servers=['kafka:9092'],
                group_id=f'attendance-service-{uuid.uuid4().hex[:8]}',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 5, 0),
                consumer_timeout_ms=2000
            )
            
            logger.info("Kafka consumer connected successfully")
            
            while self.running:
                try:
                    # Получаем сообщения
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        for tp, messages in message_batch.items():
                            for message in messages:
                                logger.info(f"Received message from topic: {message.topic}")
                                # Ставим сообщение в очередь для обработки
                                self.message_queue.put({
                                    'topic': message.topic,
                                    'value': message.value,
                                    'timestamp': message.timestamp
                                })
                    
                    # Периодически коммитим оффсеты
                    consumer.commit_async()
                    
                except Exception as e:
                    logger.error(f"Error in consumer poll: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"Kafka consumer worker error: {e}")
        finally:
            logger.info("Kafka consumer worker stopped")
    
    async def process_message_queue(self):
        """Обработка сообщений из очереди"""
        while not self.message_queue.empty():
            try:
                message = self.message_queue.get_nowait()
                await self.handle_notification_message(message)
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing queued message: {e}")
    
    async def handle_notification_message(self, message):
        """Обработка уведомлений"""
        try:
            event = message['value']
            
            # Создаем уведомление в БД
            await self.create_notification_db(
                user_id=event['user_id'],
                notification_type=event['type'],
                title=event['title'],
                message=event['message'],
                related_booking_id=event.get('related_booking_id'),
                related_workout_id=event.get('related_workout_id')
            )
            
            logger.info(f"Notification created for user {event['user_id']}")
            
        except Exception as e:
            logger.error(f"Error handling notification message: {e}")
    
    async def pg_connect(self):
        """Подключение к PostgreSQL"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"PostgreSQL connection attempt {attempt + 1}/{max_retries}")
                self.pg_conn = await pg.connect(
                    host='postgres-attendance',
                    database='attendance_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to PostgreSQL")
                
                # Создаем таблицы если их нет
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"PostgreSQL connection error: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(3)
    
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
            
            # Таблица посещений
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS attendance (
                    attendance_id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    workout_id VARCHAR(36) NOT NULL,
                    booking_id VARCHAR(36),
                    attended BOOLEAN DEFAULT FALSE,
                    checked_in_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, workout_id)
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
            
            # Вставляем тестовые данные для посещений
            await self.pg_conn.execute("""
                INSERT INTO attendance (attendance_id, user_id, workout_id, booking_id, attended, checked_in_at) VALUES
                ('attend-001', '550e8400-e29b-41d4-a716-446655440001', 'workout-001', 'booking-001', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 5 minutes'),
                ('attend-002', '550e8400-e29b-41d4-a716-446655440002', 'workout-001', 'booking-002', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 2 minutes'),
                ('attend-003', '550e8400-e29b-41d4-a716-446655440003', 'workout-001', 'booking-003', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 10 minutes'),
                ('attend-004', '550e8400-e29b-41d4-a716-446655440001', 'workout-002', 'booking-004', false, NULL),
                ('attend-005', '550e8400-e29b-41d4-a716-446655440005', 'workout-002', 'booking-005', false, NULL),
                ('attend-006', '550e8400-e29b-41d4-a716-446655440003', 'workout-003', 'booking-006', true, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '20 hours 3 minutes')
                ON CONFLICT (attendance_id) DO NOTHING
            """)
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    async def CreateNotification(self, request, context):
        """Создание уведомления через gRPC"""
        try:
            notification_id = await self.create_notification_db(
                request.user_id,
                request.type,
                request.title,
                request.message,
                request.related_booking_id if request.related_booking_id else None,
                request.related_workout_id if request.related_workout_id else None
            )
            
            return attendance_pb2.NotificationResponse(
                notification_id=notification_id,
                user_id=request.user_id,
                type=request.type,
                title=request.title,
                message=request.message,
                created_at=datetime.now().isoformat(),
                related_booking_id=request.related_booking_id if request.related_booking_id else '',
                related_workout_id=request.related_workout_id if request.related_workout_id else ''
            )
        except Exception as e:
            logger.error(f"CreateNotification error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return attendance_pb2.NotificationResponse()
    
    async def create_notification_db(self, user_id, notification_type, title, message, 
                                   related_booking_id=None, related_workout_id=None):
        """Создание уведомления в БД"""
        try:
            notification_id = str(uuid.uuid4())
            
            await self.pg_conn.execute("""
                INSERT INTO notifications (
                    notification_id, user_id, type, title, message,
                    related_booking_id, related_workout_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, notification_id, user_id, notification_type, title, message,
               related_booking_id, related_workout_id)
            
            return notification_id
        except Exception as e:
            logger.error(f"create_notification_db error: {e}")
            raise
    
    async def GetUserNotifications(self, request, context):
        """Получение уведомлений пользователя"""
        try:
            query = """
                SELECT * FROM notifications 
                WHERE user_id = $1 
                ORDER BY created_at DESC
            """
            
            if request.limit > 0:
                query += f" LIMIT {request.limit}"
            
            records = await self.pg_conn.fetch(query, request.user_id)
            
            notifications = []
            for record in records:
                notifications.append(attendance_pb2.NotificationResponse(
                    notification_id=record['notification_id'],
                    user_id=record['user_id'],
                    type=record['type'],
                    title=record['title'],
                    message=record['message'],
                    created_at=str(record['created_at']),
                    related_booking_id=record.get('related_booking_id', ''),
                    related_workout_id=record.get('related_workout_id', '')
                ))
            
            return attendance_pb2.UserNotificationsResponse(notifications=notifications)
        except Exception as e:
            logger.error(f"GetUserNotifications error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return attendance_pb2.UserNotificationsResponse()
    
    async def MarkAttendance(self, request, context):
        """Отметка посещения"""
        try:
            attendance_id = str(uuid.uuid4())
            checked_in_at = datetime.now().isoformat() if request.attended else None
            
            # Проверяем, существует ли уже запись
            existing = await self.pg_conn.fetchrow("""
                SELECT * FROM attendance 
                WHERE user_id = $1 AND workout_id = $2
            """, request.user_id, request.workout_id)
            
            if existing:
                # Обновляем существующую запись
                await self.pg_conn.execute("""
                    UPDATE attendance 
                    SET attended = $3, checked_in_at = $4
                    WHERE user_id = $1 AND workout_id = $2
                """, request.user_id, request.workout_id, 
                   request.attended, checked_in_at)
                attendance_id = existing['attendance_id']
            else:
                # Создаем новую запись
                await self.pg_conn.execute("""
                    INSERT INTO attendance (
                        attendance_id, user_id, workout_id, attended, checked_in_at
                    ) VALUES ($1, $2, $3, $4, $5)
                """, attendance_id, request.user_id, request.workout_id,
                   request.attended, checked_in_at)
            
            return attendance_pb2.SuccessResponse(
                success=True,
                message="Attendance marked successfully"
            )
        except Exception as e:
            logger.error(f"MarkAttendance error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return attendance_pb2.SuccessResponse(success=False)
    
    async def GetAttendance(self, request, context):
        """Получение информации о посещаемости"""
        try:
            records = await self.pg_conn.fetch("""
                SELECT * FROM attendance 
                WHERE workout_id = $1
            """, request.workout_id)
            
            attendance_records = []
            total = 0
            attended = 0
            
            for record in records:
                attendance_records.append(attendance_pb2.AttendanceRecord(
                    user_id=record['user_id'],
                    workout_id=record['workout_id'],
                    attended=record['attended'],
                    checked_in_at=str(record['checked_in_at']) if record['checked_in_at'] else ''
                ))
                
                total += 1
                if record['attended']:
                    attended += 1
            
            return attendance_pb2.AttendanceResponse(
                attendance=attendance_records,
                total=total,
                attended=attended
            )
        except Exception as e:
            logger.error(f"GetAttendance error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return attendance_pb2.AttendanceResponse()
    
    def stop_kafka_consumer(self):
        """Остановка Kafka Consumer"""
        logger.info("Stopping Kafka consumer...")
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
    
    async def cleanup(self):
        """Очистка ресурсов"""
        try:
            self.stop_kafka_consumer()
            if self.kafka_producer:
                self.kafka_producer.close()
            if self.pg_conn:
                await self.pg_conn.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def periodic_queue_processor(service, interval=1):
    """Периодическая обработка очереди сообщений"""
    while True:
        try:
            await service.process_message_queue()
        except Exception as e:
            logger.error(f"Error in queue processor: {e}")
        await asyncio.sleep(interval)

async def serve():
    """Запуск gRPC сервера"""
    server = grpc.aio.server()
    service = AttendanceService()
    
    try:
        # 1. Подключаемся к PostgreSQL
        await service.pg_connect()
        
        # 2. Регистрируем сервис
        attendance_pb2_grpc.add_AttendanceServiceServicer_to_server(service, server)
        server.add_insecure_port('[::]:50054')
        
        # 3. Запускаем gRPC сервер
        await server.start()
        logger.info("✅ Attendance Service gRPC server started on [::]:50054")
        
        # 4. Запускаем Kafka Consumer в отдельном потоке
        service.start_kafka_consumer_thread()
        
        # 5. Запускаем обработчик очереди сообщений
        queue_task = asyncio.create_task(periodic_queue_processor(service))
        
        # 6. Бесконечный цикл ожидания
        await server.wait_for_termination()
        
        # 7. Отменяем задачу обработки очереди
        queue_task.cancel()
        
    except Exception as e:
        logger.critical(f" Failed to start service: {e}")
        raise
    finally:
        await service.cleanup()

if __name__ == '__main__':
    try:
        logger.info("Starting Attendance Service with Kafka support...")
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info(" Service stopped by user")
    except Exception as e:
        logger.error(f" Service crashed: {e}")