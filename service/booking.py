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
import booking_pb2, booking_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BookingService')

class BookingService(booking_pb2_grpc.BookingServiceServicer):
    def __init__(self):
        logger.info('Initializing Booking Service...')
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
                'user-validated',
                'user-invalid',
                'slot-reserved',
                'slot-rejected',
                'booking-confirmed',
                'booking-cancelled',
                bootstrap_servers=['kafka:9092'],
                group_id=f'booking-service-{uuid.uuid4().hex[:8]}',
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
        """Обработка сообщений из очереди (вызывается из основного цикла)"""
        while not self.message_queue.empty():
            try:
                message = self.message_queue.get_nowait()
                await self.handle_saga_message(message)
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing queued message: {e}")
    
    async def handle_saga_message(self, message):
        """Обработка сообщений Saga"""
        try:
            event = message['value']
            topic = message['topic']
            booking_id = event.get('booking_id')
            
            logger.info(f"Processing {topic} for booking {booking_id}")
            
            if topic == 'user-validated':
                # Пользователь проверен, проверяем расписание
                if self.kafka_producer:
                    try:
                        check_event = {
                            'booking_id': booking_id,
                            'user_id': event['user_id'],
                            'workout_id': event['workout_id'],
                            'timestamp': datetime.now().isoformat()
                        }
                        future = self.kafka_producer.send('schedule-check', check_event)
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: future.get(timeout=3)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send schedule-check event: {e}")
                
            elif topic == 'user-invalid':
                # Отменяем бронирование
                await self.cancel_booking_in_db(booking_id, 'User validation failed')
                
                # Отправляем уведомление
                await self.send_notification(
                    event['user_id'],
                    'BOOKING_FAILED',
                    'Booking Failed',
                    'User validation failed',
                    booking_id
                )
            
            elif topic == 'slot-reserved':
                # Слот зарезервирован, завершаем бронирование
                success = await self.confirm_booking_in_db(booking_id)
                
                if success:
                    # Записываем в аналитику
                    await self.send_booking_recorded_event(event, booking_id)
                    
                    # Отправляем уведомление
                    await self.send_notification(
                        event['user_id'],
                        'BOOKING_CONFIRMED',
                        'Booking Confirmed',
                        'Your booking has been confirmed',
                        booking_id,
                        event['workout_id']
                    )
                    
                    # Публикуем финальное событие
                    await self.send_booking_confirmed_event(event, booking_id)
            
            elif topic == 'slot-rejected':
                # Отменяем бронирование
                await self.cancel_booking_in_db(booking_id, 'No available slots')
                
                # Отправляем уведомление
                await self.send_notification(
                    event['user_id'],
                    'BOOKING_FAILED',
                    'Booking Failed',
                    'No available slots',
                    booking_id
                )
                
                # Публикуем событие отмены
                await self.send_booking_cancelled_event(event, booking_id, 'No available slots')
        
        except Exception as e:
            logger.error(f"Error handling saga message: {e}")
    
    async def send_notification(self, user_id, notification_type, title, message, booking_id=None, workout_id=None):
        """Отправка уведомления"""
        try:
            if self.kafka_producer:
                notification_event = {
                    'user_id': user_id,
                    'type': notification_type,
                    'title': title,
                    'message': message,
                    'related_booking_id': booking_id,
                    'related_workout_id': workout_id
                }
                future = self.kafka_producer.send('notification-created', notification_event)
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: future.get(timeout=3)
                )
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")
    
    async def send_booking_recorded_event(self, event, booking_id):
        """Отправка события о записанном бронировании"""
        try:
            if self.kafka_producer:
                recorded_event = {
                    'booking_id': booking_id,
                    'user_id': event['user_id'],
                    'workout_id': event['workout_id'],
                    'booking_date': datetime.now().isoformat(),
                    'timestamp': datetime.now().isoformat()
                }
                future = self.kafka_producer.send('booking-recorded', recorded_event)
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: future.get(timeout=3)
                )
        except Exception as e:
            logger.error(f"Failed to send booking-recorded event: {e}")
    
    async def send_booking_confirmed_event(self, event, booking_id):
        """Отправка события о подтвержденном бронировании"""
        try:
            if self.kafka_producer:
                confirmed_event = {
                    'booking_id': booking_id,
                    'user_id': event['user_id'],
                    'workout_id': event['workout_id'],
                    'timestamp': datetime.now().isoformat()
                }
                future = self.kafka_producer.send('booking-confirmed', confirmed_event)
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: future.get(timeout=3)
                )
        except Exception as e:
            logger.error(f"Failed to send booking-confirmed event: {e}")
    
    async def send_booking_cancelled_event(self, event, booking_id, reason):
        """Отправка события об отмененном бронировании"""
        try:
            if self.kafka_producer:
                cancelled_event = {
                    'booking_id': booking_id,
                    'user_id': event['user_id'],
                    'workout_id': event['workout_id'],
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                }
                future = self.kafka_producer.send('booking-cancelled', cancelled_event)
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: future.get(timeout=3)
                )
        except Exception as e:
            logger.error(f"Failed to send booking-cancelled event: {e}")
    
    async def pg_connect(self):
        """Подключение к PostgreSQL"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"PostgreSQL connection attempt {attempt + 1}/{max_retries}")
                self.pg_conn = await pg.connect(
                    host='postgres-booking',
                    database='booking_db',
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
    
    async def CreateBooking(self, request, context):
        """Создание бронирования"""
        try:
            booking_id = str(uuid.uuid4())
            
            # Создаем предварительную запись о бронировании
            await self.pg_conn.execute("""
                INSERT INTO bookings (
                    booking_id, user_id, workout_id, status, created_at
                ) VALUES ($1, $2, $3, 'PENDING', CURRENT_TIMESTAMP)
            """, booking_id, request.user_id, request.workout_id)
            
            # Инициируем Saga
            if self.kafka_producer:
                try:
                    saga_event = {
                        'booking_id': booking_id,
                        'user_id': request.user_id,
                        'workout_id': request.workout_id,
                        'timestamp': datetime.now().isoformat()
                    }
                    future = self.kafka_producer.send('booking-requested', saga_event)
                    await asyncio.get_event_loop().run_in_executor(
                        None, lambda: future.get(timeout=3)
                    )
                except Exception as e:
                    logger.error(f"Failed to send booking-requested event: {e}")
            
            return booking_pb2.BookingResponse(
                booking_id=booking_id,
                user_id=request.user_id,
                workout_id=request.workout_id,
                status='PENDING',
                created_at=datetime.now().isoformat()
            )
        except Exception as e:
            logger.error(f"CreateBooking error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return booking_pb2.BookingResponse()
    
    async def CancelBooking(self, request, context):
        """Отмена бронирования"""
        try:
            success = await self.cancel_booking_in_db(request.booking_id, request.reason)
            
            if success:
                # Отправляем событие об отмене
                if self.kafka_producer:
                    try:
                        cancelled_event = {
                            'booking_id': request.booking_id,
                            'reason': request.reason,
                            'timestamp': datetime.now().isoformat()
                        }
                        future = self.kafka_producer.send('booking-cancelled', cancelled_event)
                        await asyncio.get_event_loop().run_in_executor(
                            None, lambda: future.get(timeout=3)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send booking-cancelled event: {e}")
                
                # Отправляем уведомление
                booking = await self.get_booking(request.booking_id)
                if booking:
                    await self.send_notification(
                        booking['user_id'],
                        'BOOKING_CANCELLED',
                        'Booking Cancelled',
                        f'Booking cancelled: {request.reason}',
                        request.booking_id
                    )
            
            return booking_pb2.SuccessResponse(
                success=success,
                message="Booking cancelled" if success else "Failed to cancel booking"
            )
        except Exception as e:
            logger.error(f"CancelBooking error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return booking_pb2.SuccessResponse(success=False)
    
    async def GetBooking(self, request, context):
        """Получение информации о бронировании"""
        try:
            booking = await self.get_booking(request.booking_id)
            
            if not booking:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Booking not found")
                return booking_pb2.BookingResponse()
            
            return booking_pb2.BookingResponse(
                booking_id=booking['booking_id'],
                user_id=booking['user_id'],
                workout_id=booking['workout_id'],
                status=booking['status'],
                created_at=str(booking['created_at']),
                updated_at=str(booking['updated_at']),
                reason=booking.get('reason', '')
            )
        except Exception as e:
            logger.error(f"GetBooking error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return booking_pb2.BookingResponse()
    
    async def GetUserBookings(self, request, context):
        """Получение бронирований пользователя"""
        try:
            query = "SELECT * FROM bookings WHERE user_id = $1"
            params = [request.user_id]
            
            if request.status:
                query += " AND status = $2"
                params.append(request.status)
            
            query += " ORDER BY created_at DESC"
            
            records = await self.pg_conn.fetch(query, *params)
            
            bookings = []
            for record in records:
                bookings.append(booking_pb2.BookingResponse(
                    booking_id=record['booking_id'],
                    user_id=record['user_id'],
                    workout_id=record['workout_id'],
                    status=record['status'],
                    created_at=str(record['created_at']),
                    updated_at=str(record['updated_at']),
                    reason=record.get('reason', '')
                ))
            
            return booking_pb2.UserBookingsResponse(bookings=bookings)
        except Exception as e:
            logger.error(f"GetUserBookings error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return booking_pb2.UserBookingsResponse()
    
    async def confirm_booking_in_db(self, booking_id):
        """Подтверждение бронирования в БД"""
        try:
            await self.pg_conn.execute("""
                UPDATE bookings 
                SET status = 'CONFIRMED', updated_at = CURRENT_TIMESTAMP
                WHERE booking_id = $1
            """, booking_id)
            return True
        except Exception as e:
            logger.error(f"confirm_booking_in_db error: {e}")
            return False
    
    async def cancel_booking_in_db(self, booking_id, reason):
        """Отмена бронирования в БД"""
        try:
            await self.pg_conn.execute("""
                UPDATE bookings 
                SET status = 'CANCELLED', reason = $2, updated_at = CURRENT_TIMESTAMP
                WHERE booking_id = $1
            """, booking_id, reason)
            return True
        except Exception as e:
            logger.error(f"cancel_booking_in_db error: {e}")
            return False
    
    async def get_booking(self, booking_id):
        """Получение бронирования из БД"""
        try:
            return await self.pg_conn.fetchrow(
                "SELECT * FROM bookings WHERE booking_id = $1", 
                booking_id
            )
        except Exception as e:
            logger.error(f"get_booking error: {e}")
            return None
    
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
    service = BookingService()
    
    try:
        # 1. Подключаемся к PostgreSQL
        await service.pg_connect()
        
        # 2. Регистрируем сервис
        booking_pb2_grpc.add_BookingServiceServicer_to_server(service, server)
        server.add_insecure_port('[::]:50053')
        
        # 3. Запускаем gRPC сервер
        await server.start()
        logger.info("✅ Booking Service gRPC server started on [::]:50053")
        
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
        logger.info("Starting Booking Service with Kafka Saga support...")
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info(" Service stopped by user")
    except Exception as e:
        logger.error(f" Service crashed: {e}")