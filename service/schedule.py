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
import schedule_pb2, schedule_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ScheduleService')

class ScheduleService(schedule_pb2_grpc.ScheduleServiceServicer):
    def __init__(self):
        logger.info('Initializing Schedule Service...')
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
                'schedule-check',
                bootstrap_servers=['kafka:9092'],
                group_id=f'schedule-service-{uuid.uuid4().hex[:8]}',
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
            
            if topic == 'user-validated' or topic == 'schedule-check':
                # Проверяем доступность мест
                available = await self.check_availability_db(
                    event['workout_id']
                )
                
                if available:
                    # Резервируем слот
                    success = await self.reserve_slot_db(
                        event['workout_id'],
                        slots=1
                    )
                    
                    if success and self.kafka_producer:
                        try:
                            # Публикуем событие успеха
                            slot_event = {
                                'booking_id': booking_id,
                                'user_id': event['user_id'],
                                'workout_id': event['workout_id'],
                                'timestamp': datetime.now().isoformat()
                            }
                            future = self.kafka_producer.send('slot-reserved', slot_event)
                            await asyncio.get_event_loop().run_in_executor(
                                None, lambda: future.get(timeout=3)
                            )
                            logger.info(f"Slot reserved for booking {booking_id}")
                        except Exception as e:
                            logger.error(f"Failed to send slot-reserved event: {e}")
                    else:
                        await self.send_rejection_event(event, booking_id, 'No available slots')
                else:
                    await self.send_rejection_event(event, booking_id, 'No available slots')
            
            elif topic == 'user-invalid':
                # Откатываем операцию, если была резервация
                await self.release_slot_db(
                    event.get('workout_id'),
                    slots=1
                )
        
        except Exception as e:
            logger.error(f"Error handling saga message: {e}")
    
    async def send_rejection_event(self, event, booking_id, reason):
        """Отправка события отказа"""
        try:
            if self.kafka_producer:
                rejection_event = {
                    'booking_id': booking_id,
                    'user_id': event['user_id'],
                    'workout_id': event['workout_id'],
                    'reason': reason,
                    'timestamp': datetime.now().isoformat()
                }
                future = self.kafka_producer.send('slot-rejected', rejection_event)
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: future.get(timeout=3)
                )
                logger.info(f"Slot rejected for booking {booking_id}: {reason}")
        except Exception as e:
            logger.error(f"Failed to send rejection event: {e}")
    
    async def pg_connect(self):
        """Подключение к PostgreSQL"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"PostgreSQL connection attempt {attempt + 1}/{max_retries}")
                self.pg_conn = await pg.connect(
                    host='postgres-schedule',
                    database='schedule_db',
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
            # Таблица тренировок
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS workouts (
                    workout_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    trainer_id VARCHAR(36) NOT NULL,
                    room_id VARCHAR(36) NOT NULL,
                    datetime TIMESTAMP NOT NULL,
                    max_participants INTEGER NOT NULL,
                    current_participants INTEGER DEFAULT 0,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'active'
                )
            """)
            
            # Таблица тренеров
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS trainers (
                    trainer_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    specialty VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    phone VARCHAR(20)
                )
            """)
            
            # Таблица залов
            await self.pg_conn.execute("""
                CREATE TABLE IF NOT EXISTS rooms (
                    room_id VARCHAR(36) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    capacity INTEGER NOT NULL
                )
            """)
            
            # Вставляем залы
            await self.pg_conn.execute("""
                INSERT INTO rooms (room_id, name, capacity) VALUES
                ('room-001', 'Основной зал', 20),
                ('room-002', 'Кардио-зона', 15),
                ('room-003', 'Силовая зона', 10),
                ('room-004', 'Йога-студия', 12),
                ('room-005', 'Бассейн', 8)
                ON CONFLICT (room_id) DO NOTHING
            """)
            
            # Вставляем тренеров
            await self.pg_conn.execute("""
                INSERT INTO trainers (trainer_id, name, specialty, email, phone) VALUES
                ('550e8400-e29b-41d4-a716-446655440101', 'Александр Тренеров', 'Силовая тренировка', 'alex.trener@email.com', '+79166789012'),
                ('550e8400-e29b-41d4-a716-446655440102', 'Ольга Фитнесова', 'Йога и стретчинг', 'olga.fit@email.com', '+79167890123'),
                ('550e8400-e29b-41d4-a716-446655440103', 'Максим Силов', 'Кроссфит', 'max.power@email.com', '+79168901234'),
                ('trainer-004', 'Екатерина Кардио', 'Кардио-тренировки', 'ekaterina.cardio@email.com', '+79160001122'),
                ('trainer-005', 'Артем Плавание', 'Аквааэробика', 'artem.swim@email.com', '+79160002233')
                ON CONFLICT (trainer_id) DO NOTHING
            """)
            
            # Вставляем тренировки
            await self.pg_conn.execute("""
                INSERT INTO workouts (workout_id, name, trainer_id, room_id, datetime, max_participants, current_participants, description, status) VALUES
                ('workout-001', 'Утренняя йога', '550e8400-e29b-41d4-a716-446655440102', 'room-004', CURRENT_DATE + INTERVAL '9 hours', 12, 8, 'Расслабляющая утренняя практика', 'active'),
                ('workout-002', 'Силовая тренировка', '550e8400-e29b-41d4-a716-446655440101', 'room-001', CURRENT_DATE + INTERVAL '18 hours', 15, 12, 'Проработка всех групп мышц', 'active'),
                ('workout-003', 'Кардио-интенсив', 'trainer-004', 'room-002', CURRENT_DATE + INTERVAL '20 hours', 15, 10, 'Интервальная тренировка', 'active'),
                ('workout-004', 'Кроссфит', '550e8400-e29b-41d4-a716-446655440103', 'room-003', CURRENT_DATE + INTERVAL '1 day' + INTERVAL '10 hours', 10, 6, 'Высокоинтенсивная тренировка', 'active'),
                ('workout-005', 'Аквааэробика', 'trainer-005', 'room-005', CURRENT_DATE + INTERVAL '1 day' + INTERVAL '19 hours', 8, 5, 'Тренировка в воде', 'active'),
                ('workout-006', 'Йога для начинающих', '550e8400-e29b-41d4-a716-446655440102', 'room-004', CURRENT_DATE + INTERVAL '2 days' + INTERVAL '11 hours', 12, 4, 'Базовые асаны и дыхание', 'active'),
                ('workout-007', 'Функциональный тренинг', '550e8400-e29b-41d4-a716-446655440101', 'room-001', CURRENT_DATE + INTERVAL '2 days' + INTERVAL '17 hours', 15, 9, 'Улучшение координации и силы', 'active'),
                ('workout-008', 'Вечерний стретчинг', 'trainer-004', 'room-002', CURRENT_DATE + INTERVAL '3 days' + INTERVAL '20 hours', 15, 2, 'Растяжка после рабочего дня', 'cancelled')
                ON CONFLICT (workout_id) DO NOTHING
            """)
            
            logger.info("Database tables created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    async def CreateWorkout(self, request, context):
        """Создание тренировки"""
        try:
            workout_id = str(uuid.uuid4())

            # Валидация datetime
            if not request.datetime:
                raise ValueError("Datetime is required")

            # Пытаемся преобразовать строку в datetime
            try:
                # Проверяем формат даты
                workout_datetime = datetime.fromisoformat(request.datetime.replace('Z', '+00:00'))
            except ValueError:
                raise ValueError(f"Invalid datetime format: {request.datetime}. Expected ISO format")

            # Валидация других полей
            if not request.name or not request.trainer_id or not request.room_id:
                raise ValueError("Name, trainer_id and room_id are required")

            if request.max_participants <= 0:
                raise ValueError("max_participants must be positive")

            await self.pg_conn.execute("""
                INSERT INTO workouts (
                    workout_id, name, trainer_id, room_id, 
                    datetime, max_participants, description, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active')
            """, workout_id, request.name, request.trainer_id, 
               request.room_id, workout_datetime, 
               request.max_participants, request.description or "")

            logger.info(f"Workout created: {workout_id} ({request.name})")

            return schedule_pb2.WorkoutResponse(
                workout_id=workout_id,
                name=request.name,
                trainer_id=request.trainer_id,
                room_id=request.room_id,
                datetime=workout_datetime.isoformat(),
                max_participants=request.max_participants,
                current_participants=0,
                description=request.description or "",
                status='active'
            )
        except ValueError as e:
            logger.error(f"CreateWorkout validation error: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return schedule_pb2.WorkoutResponse()
        except Exception as e:
            logger.error(f"CreateWorkout error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.WorkoutResponse()
    
    async def GetWorkout(self, request, context):
        """Получение информации о тренировке"""
        try:
            record = await self.pg_conn.fetchrow(
                "SELECT * FROM workouts WHERE workout_id = $1", 
                request.workout_id
            )
            
            if not record:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Workout not found")
                return schedule_pb2.WorkoutResponse()
            
            return schedule_pb2.WorkoutResponse(
                workout_id=record['workout_id'],
                name=record['name'],
                trainer_id=record['trainer_id'],
                room_id=record['room_id'],
                datetime=str(record['datetime']),
                max_participants=record['max_participants'],
                current_participants=record['current_participants'],
                description=record['description'],
                status=record['status']
            )
        except Exception as e:
            logger.error(f"GetWorkout error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.WorkoutResponse()
    
    async def ListWorkouts(self, request, context):
        """Список тренировок"""
        try:
            query = "SELECT * FROM workouts WHERE status = 'active'"
            params = []
            
            if request.date_from:
                query += " AND datetime >= $1"
                # Валидация datetime
                try:
                    # Проверяем формат даты
                    workout_datetime = datetime.fromisoformat(request.date_from.replace('Z', '+00:00'))
                except ValueError:
                    raise ValueError(f"Invalid datetime format: {request.date_from}. Expected ISO format")
            
                params.append(workout_datetime)
            
            if request.date_to:
                if len(params) == 0:
                    query += " AND datetime <= $1"
                else:
                    query += " AND datetime <= $2"
                # Валидация datetime
                try:
                    # Проверяем формат даты
                    workout_datetime = datetime.fromisoformat(request.date_to.replace('Z', '+00:00'))
                except ValueError:
                    raise ValueError(f"Invalid datetime format: {request.date_to}. Expected ISO format")
            
                params.append(workout_datetime)
            
            if request.trainer_id:
                if len(params) == 0:
                    query += " AND trainer_id = $1"
                elif len(params) == 1:
                    query += " AND trainer_id = $2"
                else:
                    query += " AND trainer_id = $3"
                params.append(request.trainer_id)
            
            if request.room_id:
                if len(params) == 0:
                    query += " AND room_id = $1"
                elif len(params) == 1:
                    query += " AND room_id = $2"
                elif len(params) == 2:
                    query += " AND room_id = $3"
                else:
                    query += " AND room_id = $4"
                params.append(request.room_id)
            
            query += " ORDER BY datetime"
            
            records = await self.pg_conn.fetch(query, *params)
            
            workouts = []
            for record in records:
                workouts.append(schedule_pb2.WorkoutResponse(
                    workout_id=record['workout_id'],
                    name=record['name'],
                    trainer_id=record['trainer_id'],
                    room_id=record['room_id'],
                    datetime=str(record['datetime']),
                    max_participants=record['max_participants'],
                    current_participants=record['current_participants'],
                    description=record['description'],
                    status=record['status']
                ))
            
            return schedule_pb2.ListWorkoutsResponse(workouts=workouts)
        except Exception as e:
            logger.error(f"ListWorkouts error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.ListWorkoutsResponse()
    
    async def CheckAvailability(self, request, context):
        """Проверка доступности мест"""
        try:
            available = await self.check_availability_db(request.workout_id)
            
            return schedule_pb2.AvailabilityResponse(
                available=available,
                available_slots=1 if available else 0
            )
        except Exception as e:
            logger.error(f"CheckAvailability error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.AvailabilityResponse()
    
    async def check_availability_db(self, workout_id):
        """Проверка доступности мест в базе данных"""
        try:
            record = await self.pg_conn.fetchrow(
                "SELECT max_participants, current_participants FROM workouts WHERE workout_id = $1",
                workout_id
            )
            
            if not record:
                return False
            
            return record['current_participants'] < record['max_participants']
        except Exception as e:
            logger.error(f"check_availability_db error: {e}")
            return False
    
    async def ReserveSlot(self, request, context):
        """Резервирование места"""
        try:
            success = await self.reserve_slot_db(request.workout_id, request.slots)
            
            return schedule_pb2.ReserveSlotResponse(
                success=success,
                message="Slot reserved" if success else "Failed to reserve slot"
            )
        except Exception as e:
            logger.error(f"ReserveSlot error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.ReserveSlotResponse(success=False)
    
    async def reserve_slot_db(self, workout_id, slots=1):
        """Резервирование места в базе данных"""
        try:
            await self.pg_conn.execute("""
                UPDATE workouts 
                SET current_participants = current_participants + $1
                WHERE workout_id = $2 
                AND (current_participants + $1) <= max_participants
            """, slots, workout_id)
            
            return True
        except Exception as e:
            logger.error(f"reserve_slot_db error: {e}")
            return False
    
    async def ReleaseSlot(self, request, context):
        """Освобождение места"""
        try:
            await self.release_slot_db(request.workout_id, request.slots)
            
            return schedule_pb2.SuccessResponse(
                success=True,
                message="Slot released"
            )
        except Exception as e:
            logger.error(f"ReleaseSlot error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return schedule_pb2.SuccessResponse(success=False)
    
    async def release_slot_db(self, workout_id, slots=1):
        """Освобождение места в базе данных"""
        try:
            await self.pg_conn.execute("""
                UPDATE workouts 
                SET current_participants = GREATEST(0, current_participants - $1)
                WHERE workout_id = $2
            """, slots, workout_id)
        except Exception as e:
            logger.error(f"release_slot_db error: {e}")
    
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
    service = ScheduleService()
    
    try:
        # 1. Подключаемся к PostgreSQL
        await service.pg_connect()
        
        # 2. Регистрируем сервис
        schedule_pb2_grpc.add_ScheduleServiceServicer_to_server(service, server)
        server.add_insecure_port('[::]:50051')
        
        # 3. Запускаем gRPC сервер
        await server.start()
        logger.info("✅ Schedule Service gRPC server started on [::]:50051")
        
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
        logger.info("Starting Schedule Service with Kafka Saga support...")
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info(" Service stopped by user")
    except Exception as e:
        logger.error(f" Service crashed: {e}")