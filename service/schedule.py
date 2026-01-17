import asyncio
import asyncpg
import grpc
from concurrent import futures
import logging
from datetime import datetime

import schedule_pb2
import schedule_pb2_grpc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ScheduleService')

class ScheduleService(schedule_pb2_grpc.ScheduleServiceServicer):
    
    def __init__(self):
        self.pg_conn = None
        
    async def pg_connect(self):
        for _ in range(5):
            try:
                self.pg_conn = await asyncpg.connect(
                    host='postgres-schedule',
                    database='schedule_db',
                    user='postgres',
                    password='postgres',
                    port=5432
                )
                logger.info("Successfully connected to schedule database")
                await self.setup_tables()
                break
            except Exception as e:
                logger.error(f"Database connection error, attempt {_}: {e}")
                await asyncio.sleep(5)
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
    async def GetWorkout(self, request, context):
        try:
            workout = await self.pg_conn.fetchrow(
                "SELECT * FROM workouts WHERE workout_id = $1",
                request.workout_id
            )
            
            if not workout:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('Workout not found')
                return schedule_pb2.WorkoutResponse()
            
            return schedule_pb2.WorkoutResponse(
                workout_id=str(workout['workout_id']),
                name=str(workout['name']),
                trainer_id=str(workout['trainer_id']),
                room_id=str(workout['room_id']),
                datetime=str(workout['datetime']),
                max_participants=int(workout['max_participants']),
                current_participants=int(workout['current_participants']),
                description=str(workout['description']),
                status=str(workout['status'])
            )
        except Exception as e:
            logger.error(f'GetWorkout failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetWorkout failed: {str(e)}')
            return schedule_pb2.WorkoutResponse()
    
    async def GetWorkouts(self, request, context):
        try:
            # Преобразуем строки дат в datetime объекты
            date_from = None
            date_to = None
            
            if request.date_from:
                try:
                    date_from = datetime.strptime(request.date_from, '%Y-%m-%d')
                except ValueError:
                    # Если передано с временем, пробуем другой формат
                    try:
                        date_from = datetime.strptime(request.date_from, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            date_from = datetime.strptime(request.date_from, '%Y-%m-%dT%H:%M:%S')
                        except ValueError as e:
                            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                            context.set_details(f"Invalid date_from format: {request.date_from}")
                            return schedule_pb2.WorkoutsResponse()
            
            if request.date_to:
                try:
                    date_to = datetime.strptime(request.date_to, '%Y-%m-%d')
                except ValueError:
                    try:
                        date_to = datetime.strptime(request.date_to, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            date_to = datetime.strptime(request.date_to, '%Y-%m-%dT%H:%M:%S')
                        except ValueError as e:
                            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                            context.set_details(f"Invalid date_to format: {request.date_to}")
                            return schedule_pb2.WorkoutsResponse()
            
            # Если не указаны даты, используем сегодня и неделю вперед
            if not date_from:
                date_from = datetime.now()
            if not date_to:
                date_to = date_from + timedelta(days=7)
            
            workouts = await self.pg_conn.fetch(
                """
                SELECT * FROM workouts 
                WHERE datetime >= $1::timestamp 
                AND datetime <= $2::timestamp 
                AND status = 'active'
                ORDER BY datetime
                """,
                date_from,
                date_to
            )
            
            response = schedule_pb2.WorkoutsResponse()
            for workout in workouts:
                response.workouts.append(schedule_pb2.WorkoutResponse(
                    workout_id=str(workout['workout_id']),
                    name=str(workout['name']),
                    trainer_id=str(workout['trainer_id']),
                    room_id=str(workout['room_id']),
                    datetime=str(workout['datetime']),
                    max_participants=int(workout['max_participants']),
                    current_participants=int(workout['current_participants']),
                    description=str(workout['description']),
                    status=str(workout['status'])
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetWorkouts failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetWorkouts failed: {str(e)}')
            return schedule_pb2.WorkoutsResponse()
    
    async def ReserveSlot(self, request, context):
        try:
            async with self.pg_conn.transaction():
                # Check current participants
                workout = await self.pg_conn.fetchrow(
                    "SELECT current_participants, max_participants FROM workouts WHERE workout_id = $1",
                    request.workout_id
                )
                
                if not workout:
                    return schedule_pb2.ReserveSlotResponse(
                        success=False,
                        message='Workout not found'
                    )
                
                if workout['current_participants'] >= workout['max_participants']:
                    return schedule_pb2.ReserveSlotResponse(
                        success=False,
                        message='No available slots'
                    )
                
                # Increment participants
                await self.pg_conn.execute(
                    """
                    UPDATE workouts 
                    SET current_participants = current_participants + 1,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE workout_id = $1
                    """,
                    request.workout_id
                )
                
                # Get updated count
                updated = await self.pg_conn.fetchrow(
                    "SELECT current_participants FROM workouts WHERE workout_id = $1",
                    request.workout_id
                )
                
                return schedule_pb2.ReserveSlotResponse(
                    success=True,
                    message='Slot reserved successfully',
                    current_participants=updated['current_participants']
                )
        except Exception as e:
            logger.error(f'ReserveSlot failed: {str(e)}')
            return schedule_pb2.ReserveSlotResponse(
                success=False,
                message=f'ReserveSlot failed: {str(e)}'
            )
    
    async def CancelReservation(self, request, context):
        try:
            async with self.pg_conn.transaction():
                # Decrement participants (but not below 0)
                await self.pg_conn.execute(
                    """
                    UPDATE workouts 
                    SET current_participants = GREATEST(current_participants - 1, 0),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE workout_id = $1
                    """,
                    request.workout_id
                )
                
                return schedule_pb2.SuccessResponse(
                    success=True,
                    message='Reservation cancelled'
                )
        except Exception as e:
            logger.error(f'CancelReservation failed: {str(e)}')
            return schedule_pb2.SuccessResponse(
                success=False,
                message=f'CancelReservation failed: {str(e)}'
            )
    
    async def CreateWorkout(self, request, context):
        try:
            workout_id = f'workout-{datetime.now().strftime("%Y%m%d%H%M%S")}'

            # Преобразуем строку datetime в timestamp
            workout_datetime = None
            if request.datetime:
                try:
                    # Пробуем разные форматы даты
                    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
                        try:
                            workout_datetime = datetime.strptime(request.datetime, fmt)
                            break
                        except ValueError:
                            continue
                    if not workout_datetime:
                        raise ValueError(f"Invalid datetime format: {request.datetime}")
                except Exception as e:
                    logger.error(f"Error parsing datetime: {e}")
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Invalid datetime format: {request.datetime}")
                    return schedule_pb2.WorkoutResponse()

            await self.pg_conn.execute(
                """
                INSERT INTO workouts 
                (workout_id, name, trainer_id, room_id, datetime, max_participants, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                workout_id,
                request.name,
                request.trainer_id,
                request.room_id,
                workout_datetime,  # Теперь это datetime объект
                request.max_participants,
                request.description
            )

            return await self.GetWorkout(
                schedule_pb2.GetWorkoutRequest(workout_id=workout_id),
                context
            )
        except Exception as e:
            logger.error(f'CreateWorkout failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'CreateWorkout failed: {str(e)}')
            return schedule_pb2.WorkoutResponse()
    
    async def GetTrainers(self, request, context):
        try:
            trainers = await self.pg_conn.fetch("SELECT * FROM trainers")
            
            response = schedule_pb2.TrainersResponse()
            for trainer in trainers:
                response.trainers.append(schedule_pb2.TrainerResponse(
                    trainer_id=str(trainer['trainer_id']),
                    name=str(trainer['name']),
                    specialty=str(trainer['specialty']),
                    email=str(trainer['email']),
                    phone=str(trainer['phone'])
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetTrainers failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetTrainers failed: {str(e)}')
            return schedule_pb2.TrainersResponse()
    
    async def GetRooms(self, request, context):
        try:
            rooms = await self.pg_conn.fetch("SELECT * FROM rooms")
            
            response = schedule_pb2.RoomsResponse()
            for room in rooms:
                response.rooms.append(schedule_pb2.RoomResponse(
                    room_id=str(room['room_id']),
                    name=str(room['name']),
                    capacity=room['capacity']
                ))
            
            return response
        except Exception as e:
            logger.error(f'GetRooms failed: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'GetRooms failed: {str(e)}')
            return schedule_pb2.RoomsResponse()

async def serve():
    server = grpc.aio.server()
    service = ScheduleService()
    await service.pg_connect()
    schedule_pb2_grpc.add_ScheduleServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50051')
    
    await server.start()
    logger.info("Schedule Service started on port 50051")
    
    await server.wait_for_termination()

if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Schedule Service stopped by user")
    except Exception as e:
        logger.error(f"Schedule Service crashed: {str(e)}")