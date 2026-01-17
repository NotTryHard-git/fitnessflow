import grpc
import asyncio
import asyncpg as pg
import json
import uuid
from datetime import datetime, timedelta
import logging
import os
import threading
import time
import queue
from kafka import KafkaProducer, KafkaConsumer
import analytics_pb2, analytics_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('AnalyticsService')

class AnalyticsService(analytics_pb2_grpc.AnalyticsServiceServicer):
    def __init__(self):
        logger.info('Initializing Analytics Service...')
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
                'booking-recorded',
                bootstrap_servers=['kafka:9092'],
                group_id=f'analytics-service-{uuid.uuid4().hex[:8]}',
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
                await self.handle_booking_recorded_message(message)
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing queued message: {e}")
    
    async def handle_booking_recorded_message(self, message):
        """Обработка записей о бронированиях"""
        try:
            event = message['value']
            
            # Записываем бронирование в статистику
            success = await self.record_booking_db(
                user_id=event['user_id'],
                workout_id=event.get('workout_id'),
                booking_date=event['booking_date']
            )
            
            if success:
                logger.info(f"Booking recorded for user {event['user_id']}")
            else:
                logger.warning(f"Failed to record booking for user {event['user_id']}")
            
        except Exception as e:
            logger.error(f"Error handling booking recorded message: {e}")
    
    async def pg_connect(self):
        """Подключение к PostgreSQL"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"PostgreSQL connection attempt {attempt + 1}/{max_retries}")
                self.pg_conn = await pg.connect(
                    host='postgres-analytics',
                    database='analytics_db',
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
            # Таблица записей бронирований
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS booking_records (
                    id SERIAL PRIMARY KEY,
                    month_year VARCHAR(7) NOT NULL,
                    user_id VARCHAR(36) NOT NULL,
                    workout_id VARCHAR(36),
                    booking_date TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица месячной статистики
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS monthly_statistics (
                    id SERIAL PRIMARY KEY,
                    month_year VARCHAR(7) NOT NULL,
                    total_bookings INTEGER DEFAULT 0,
                    unique_users INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(month_year)
                )
            """)
            
            # Вставляем тестовые данные
            await self.pg_conn.execute("""
                INSERT INTO monthly_statistics (month_year, total_bookings, unique_users) VALUES
                (TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM'), 42, 28),
                (TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYY-MM'), 58, 35),
                (TO_CHAR(CURRENT_DATE, 'YYYY-MM'), 12, 8)
                ON CONFLICT (month_year) DO NOTHING
            """)
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    async def RecordBooking(self, request, context):
        """Запись бронирования через gRPC"""
        try:
            success = await self.record_booking_db(
                request.user_id,
                request.workout_id,
                request.booking_date
            )
            
            return analytics_pb2.SuccessResponse(
                success=success,
                message="Booking recorded" if success else "Failed to record booking"
            )
        except Exception as e:
            logger.error(f"RecordBooking error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return analytics_pb2.SuccessResponse(success=False)
    
    async def record_booking_db(self, user_id, workout_id, booking_date):
        """Запись бронирования в статистику"""
        try:
            # Извлекаем месяц и год
            dt = datetime.fromisoformat(booking_date.replace('Z', '+00:00'))
            month_year = dt.strftime('%Y-%m')
            
            # Проверяем, есть ли запись за этот месяц
            record = await self.pg_conn.fetchrow("""
                SELECT * FROM monthly_statistics 
                WHERE month_year = $1
            """, month_year)
            
            if record:
                # Обновляем существующую запись
                unique_users = record['unique_users']
                
                # Проверяем, был ли этот пользователь уже учтен
                user_check = await self.pg_conn.fetchrow("""
                    SELECT 1 FROM booking_records 
                    WHERE month_year = $1 AND user_id = $2
                """, month_year, user_id)
                
                if not user_check:
                    unique_users += 1
                    # Сохраняем запись о пользователе
                    await self.pg_conn.execute("""
                        INSERT INTO booking_records (month_year, user_id, workout_id, booking_date)
                        VALUES ($1, $2, $3, $4)
                    """, month_year, user_id, workout_id, booking_date)
                
                await self.pg_conn.execute("""
                    UPDATE monthly_statistics 
                    SET total_bookings = total_bookings + 1,
                        unique_users = $2,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE month_year = $1
                """, month_year, unique_users)
            else:
                # Создаем новую запись
                await self.pg_conn.execute("""
                    INSERT INTO monthly_statistics (month_year, total_bookings, unique_users)
                    VALUES ($1, 1, 1)
                """, month_year)
                
                # Сохраняем запись о бронировании
                await self.pg_conn.execute("""
                    INSERT INTO booking_records (month_year, user_id, workout_id, booking_date)
                    VALUES ($1, $2, $3, $4)
                """, month_year, user_id, workout_id, booking_date)
            
            return True
        except Exception as e:
            logger.error(f"record_booking_db error: {e}")
            return False
    
    async def GetMonthlyStatistics(self, request, context):
        """Получение месячной статистики"""
        try:
            month_year = request.month_year
            
            if not month_year:
                # Если месяц не указан, берем текущий
                month_year = datetime.now().strftime('%Y-%m')
            
            record = await self.pg_conn.fetchrow("""
                SELECT * FROM monthly_statistics 
                WHERE month_year = $1
            """, month_year)
            
            if not record:
                # Возвращаем пустую статистику
                return analytics_pb2.MonthlyStatisticsResponse(
                    month_year=month_year,
                    total_bookings=0,
                    unique_users=0,
                    booking_growth=0.0
                )
            
            # Рассчитываем рост по сравнению с предыдущим месяцем
            prev_month = (datetime.strptime(month_year, '%Y-%m') - timedelta(days=31)).strftime('%Y-%m')
            prev_record = await self.pg_conn.fetchrow("""
                SELECT total_bookings FROM monthly_statistics 
                WHERE month_year = $1
            """, prev_month)
            
            booking_growth = 0.0
            if prev_record and prev_record['total_bookings'] > 0:
                booking_growth = ((record['total_bookings'] - prev_record['total_bookings']) / 
                                 prev_record['total_bookings'] * 100)
            
            return analytics_pb2.MonthlyStatisticsResponse(
                month_year=record['month_year'],
                total_bookings=record['total_bookings'],
                unique_users=record['unique_users'],
                booking_growth=booking_growth
            )
        except Exception as e:
            logger.error(f"GetMonthlyStatistics error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return analytics_pb2.MonthlyStatisticsResponse()
    
    async def GetWorkoutStatistics(self, request, context):
        """Получение статистики по тренировкам"""
        try:
            workout_id = request.workout_id
            
            # Получаем общую статистику по тренировке
            total_bookings = await self.pg_conn.fetchval("""
                SELECT COUNT(*) FROM booking_records 
                WHERE workout_id = $1
            """, workout_id)
            
            # Получаем количество уникальных пользователей
            unique_users = await self.pg_conn.fetchval("""
                SELECT COUNT(DISTINCT user_id) FROM booking_records 
                WHERE workout_id = $1
            """, workout_id)
            
            # Для простоты возвращаем фиктивные данные
            # В реальном приложении здесь была бы сложная логика расчета
            
            return analytics_pb2.WorkoutStatisticsResponse(
                workout_id=workout_id,
                workout_name=f"Workout {workout_id}",
                total_bookings=total_bookings or 0,
                attendance_rate=75.5,
                avg_participants=8.2
            )
        except Exception as e:
            logger.error(f"GetWorkoutStatistics error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return analytics_pb2.WorkoutStatisticsResponse()
    
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
    service = AnalyticsService()
    
    try:
        # 1. Подключаемся к PostgreSQL
        await service.pg_connect()
        
        # 2. Регистрируем сервис
        analytics_pb2_grpc.add_AnalyticsServiceServicer_to_server(service, server)
        server.add_insecure_port('[::]:50055')
        
        # 3. Запускаем gRPC сервер
        await server.start()
        logger.info("✅ Analytics Service gRPC server started on [::]:50055")
        
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
        logger.info("Starting Analytics Service with Kafka support...")
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info(" Service stopped by user")
    except Exception as e:
        logger.error(f" Service crashed: {e}")