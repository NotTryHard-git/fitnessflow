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
import user_pb2, user_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('UserService')

class UserService(user_pb2_grpc.UserServiceServicer):
    def __init__(self):
        logger.info('Initializing User Service...')
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
                max_block_ms=5000  # Уменьшаем таймаут
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
            daemon=True  # Демонический поток, завершается с основной программой
        )
        self.consumer_thread.start()
    
    def kafka_consumer_worker(self):
        """Рабочая функция для потока Consumer"""
        logger.info("Kafka consumer worker started")
        
        try:
            # Инициализируем Consumer в этом потоке
            consumer = KafkaConsumer(
                'booking-requested',
                bootstrap_servers=['kafka:9092'],
                group_id=f'user-service-{uuid.uuid4().hex[:8]}',
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                api_version=(2, 5, 0),
                consumer_timeout_ms=2000  # Увеличиваем таймаут
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
                                self.message_queue.put(message.value)
                    
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
        """Обработка сообщений из очереди (вызывается из основного цикла)"""
        while not self.message_queue.empty():
            try:
                message = self.message_queue.get_nowait()
                await self.handle_booking_request(message)
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing queued message: {e}")
    
    async def handle_booking_request(self, event):
        """Обработка запроса на бронирование (шаг Saga)"""
        try:
            user_id = event.get('user_id')
            booking_id = event.get('booking_id')
            workout_id = event.get('workout_id')
            
            if not all([user_id, booking_id, workout_id]):
                logger.warning(f"Incomplete booking event: {event}")
                return
            
            logger.info(f"Validating user {user_id} for booking {booking_id}")
            
            # Валидируем пользователя
            is_valid = await self.validate_user_for_saga(user_id)
            
            # Отправляем результат валидации
            if self.kafka_producer:
                try:
                    if is_valid:
                        validation_event = {
                            'booking_id': booking_id,
                            'user_id': user_id,
                            'workout_id': workout_id,
                            'timestamp': datetime.now().isoformat(),
                            'status': 'validated'
                        }
                        future = self.kafka_producer.send('user-validated', validation_event)
                        # Неблокирующее ожидание
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: future.get(timeout=3)
                        )
                        logger.info(f"User {user_id} validated for booking {booking_id}")
                    else:
                        rejection_event = {
                            'booking_id': booking_id,
                            'user_id': user_id,
                            'workout_id': workout_id,
                            'timestamp': datetime.now().isoformat(),
                            'status': 'rejected',
                            'reason': 'User validation failed'
                        }
                        future = self.kafka_producer.send('user-invalid', rejection_event)
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: future.get(timeout=3)
                        )
                        logger.info(f"User {user_id} rejected for booking {booking_id}")
                except Exception as e:
                    logger.error(f"Failed to send Kafka message: {e}")
        
        except Exception as e:
            logger.error(f"Error handling booking request: {e}")
    
    async def validate_user_for_saga(self, user_id):
        """Валидация пользователя для Saga"""
        try:
            record = await self.pg_conn.fetchrow(
                "SELECT status, type_id, subscription_end FROM users WHERE user_id = $1", 
                user_id
            )
            
            if not record:
                logger.warning(f"User {user_id} not found")
                return False
            
            # Проверяем статус
            if record['status'] != 'active':
                logger.warning(f"User {user_id} is not active")
                return False
            
            # Проверяем тип (должен быть клиентом)
            if record['type_id'] != 1:
                logger.warning(f"User {user_id} is not a client")
                return False
            
            # Проверяем подписку
            if record['subscription_end']:
                subscription_end = record['subscription_end']
                current_date = datetime.now().date()
                
                if subscription_end < current_date:
                    logger.warning(f"User {user_id} subscription expired")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating user {user_id}: {e}")
            return False
    
    async def pg_connect(self):
        """Подключение к PostgreSQL"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"PostgreSQL connection attempt {attempt + 1}/{max_retries}")
                self.pg_conn = await pg.connect(
                    host='postgres-user',
                    database='user_db',
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
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS user_type (
                    type_id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL UNIQUE
                )
            """)
            
            await self.pg_conn.execute("""
                INSERT INTO user_type (type_id, name) 
                VALUES 
                    (1, 'client'),
                    (2, 'trainer'),
                    (3, 'admin')
                ON CONFLICT (type_id) DO NOTHING
            """)
            
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id VARCHAR(36) PRIMARY KEY,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    first_name VARCHAR(100) NOT NULL,
                    last_name VARCHAR(100) NOT NULL,
                    phone VARCHAR(20),
                    type_id INTEGER REFERENCES user_type(type_id) DEFAULT 1,
                    subscription_end DATE,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Вставляем пользователей
            await self.pg_conn.execute("""
                -- Вставляем пользователей
                INSERT INTO users (user_id, email, first_name, last_name, phone, type_id, subscription_end, status) VALUES
                -- Клиенты
                ('550e8400-e29b-41d4-a716-446655440001', 'ivan.ivanov@email.com', 'Иван', 'Иванов', '+79161234567', 1, '2024-12-31', 'active'),
                ('550e8400-e29b-41d4-a716-446655440002', 'maria.petrova@email.com', 'Мария', 'Петрова', '+79162345678', 1, '2024-11-30', 'active'),
                ('550e8400-e29b-41d4-a716-446655440003', 'alex.smirnov@email.com', 'Алексей', 'Смирнов', '+79163456789', 1, '2024-10-15', 'active'),
                ('550e8400-e29b-41d4-a716-446655440004', 'anna.kuznetsova@email.com', 'Анна', 'Кузнецова', '+79164567890', 1, '2024-09-20', 'inactive'),
                ('550e8400-e29b-41d4-a716-446655440005', 'dmitry.vorobev@email.com', 'Дмитрий', 'Воробьев', '+79165678901', 1, '2024-12-15', 'active'),

                -- Тренеры (также есть в таблице users)
                ('550e8400-e29b-41d4-a716-446655440101', 'alex.trener@email.com', 'Александр', 'Тренеров', '+79166789012', 2, NULL, 'active'),
                ('550e8400-e29b-41d4-a716-446655440102', 'olga.fit@email.com', 'Ольга', 'Фитнесова', '+79167890123', 2, NULL, 'active'),
                ('550e8400-e29b-41d4-a716-446655440103', 'max.power@email.com', 'Максим', 'Силов', '+79168901234', 2, NULL, 'active'),

                -- Администратор
                ('550e8400-e29b-41d4-a716-446655440201', 'admin@gym.com', 'Админ', 'Админов', '+79169012345', 3, NULL, 'active')
                ON CONFLICT (user_id) DO NOTHING;
            """)
            logger.info("Inserted users")
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    async def CreateUser(self, request, context):
        """Создание пользователя"""
        try:
            user_id = str(uuid.uuid4())
            
            type_id_map = {'client': 1, 'trainer': 2, 'admin': 3}
            type_id = type_id_map.get(request.user_type, 1)
            
            # Обработка subscription_end
            subscription_end = None
            if request.subscription_end:
                try:
                    subscription_end = datetime.strptime(
                        request.subscription_end, 
                        '%Y-%m-%d'
                    ).date()
                except ValueError:
                    logger.error(f"Invalid subscription_end format: {request.subscription_end}")
                    subscription_end = None
            
            await self.pg_conn.execute("""
                INSERT INTO users (
                    user_id, email, first_name, last_name, 
                    phone, type_id, subscription_end, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
            """, user_id, request.email, request.first_name, 
               request.last_name, request.phone, type_id,
               subscription_end)
            
            logger.info(f"User created: {user_id} ({request.email})")
            
            return user_pb2.UserResponse(
                user_id=user_id,
                email=request.email,
                first_name=request.first_name,
                last_name=request.last_name,
                phone=request.phone,
                user_type=request.user_type,
                subscription_end=request.subscription_end if request.subscription_end else '',
                status='active'
            )
        except Exception as e:
            logger.error(f"CreateUser error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return user_pb2.UserResponse()
    
    async def GetUser(self, request, context):
        """Получение информации о пользователе"""
        try:
            record = await self.pg_conn.fetchrow(
                "SELECT * FROM users WHERE user_id = $1", 
                request.user_id
            )
            
            if not record:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("User not found")
                return user_pb2.UserResponse()
            
            type_record = await self.pg_conn.fetchrow(
                "SELECT name FROM user_type WHERE type_id = $1",
                record['type_id']
            )
            
            return user_pb2.UserResponse(
                user_id=record['user_id'],
                email=record['email'],
                first_name=record['first_name'],
                last_name=record['last_name'],
                phone=record['phone'],
                user_type=type_record['name'] if type_record else 'client',
                subscription_end=str(record['subscription_end']) if record['subscription_end'] else '',
                status=record['status']
            )
        except Exception as e:
            logger.error(f"GetUser error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return user_pb2.UserResponse()
    
    async def ValidateUser(self, request, context):
        """Валидация пользователя (gRPC метод)"""
        try:
            is_valid = await self.validate_user_for_saga(request.user_id)
            
            return user_pb2.ValidateUserResponse(
                valid=is_valid,
                message="User validated" if is_valid else "User validation failed"
            )
        except Exception as e:
            logger.error(f"ValidateUser error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return user_pb2.ValidateUserResponse(valid=False)
    
    async def Authenticate(self, request, context):
        """Аутентификация"""
        try:
            record = await self.pg_conn.fetchrow(
                "SELECT user_id, type_id FROM users WHERE email = $1", 
                request.email
            )
            
            if not record:
                return user_pb2.AuthResponse(success=False)
            
            token = str(uuid.uuid4())
            
            type_record = await self.pg_conn.fetchrow(
                "SELECT name FROM user_type WHERE type_id = $1",
                record['type_id']
            )
            
            return user_pb2.AuthResponse(
                success=True,
                token=token,
                user_id=record['user_id'],
                user_type=type_record['name'] if type_record else 'client'
            )
        except Exception as e:
            logger.error(f"Authenticate error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return user_pb2.AuthResponse(success=False)
    
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
    service = UserService()
    
    try:
        # 1. Подключаемся к PostgreSQL
        await service.pg_connect()
        
        # 2. Регистрируем сервис
        user_pb2_grpc.add_UserServiceServicer_to_server(service, server)
        server.add_insecure_port('[::]:50052')
        
        # 3. Запускаем gRPC сервер
        await server.start()
        logger.info("✅ User Service gRPC server started on [::]:50052")
        
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
        logger.info("Starting User Service with Kafka Saga support...")
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info(" Service stopped by user")
    except Exception as e:
        logger.error(f" Service crashed: {e}")