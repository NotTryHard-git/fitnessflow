-- Инициализация базы данных Schedule Service
CREATE DATABASE schedule_db;
\c schedule_db;

-- Таблица тренировок
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
);

CREATE TABLE IF NOT EXISTS trainers (
    trainer_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    specialty  VARCHAR(36) NOT NULL,
    email  VARCHAR(36) NOT NULL,
    phone  VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS rooms (
    room_id VARCHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    capacity   INTEGER NOT NULL
);

-- Вставляем залы
INSERT INTO rooms (room_id, name, capacity) VALUES
('room-001', 'Основной зал', 20),
('room-002', 'Кардио-зона', 15),
('room-003', 'Силовая зона', 10),
('room-004', 'Йога-студия', 12),
('room-005', 'Бассейн', 8)
ON CONFLICT (room_id) DO NOTHING;

-- Вставляем тренеров
INSERT INTO trainers (trainer_id, name, specialty, email, phone) VALUES
('550e8400-e29b-41d4-a716-446655440101', 'Александр Тренеров', 'Силовая тренировка', 'alex.trener@email.com', '+79166789012'),
('550e8400-e29b-41d4-a716-446655440102', 'Ольга Фитнесова', 'Йога и стретчинг', 'olga.fit@email.com', '+79167890123'),
('550e8400-e29b-41d4-a716-446655440103', 'Максим Силов', 'Кроссфит', 'max.power@email.com', '+79168901234'),
('trainer-004', 'Екатерина Кардио', 'Кардио-тренировки', 'ekaterina.cardio@email.com', '+79160001122'),
('trainer-005', 'Артем Плавание', 'Аквааэробика', 'artem.swim@email.com', '+79160002233')
ON CONFLICT (trainer_id) DO NOTHING;

-- Вставляем тренировки на ближайшие 7 дней
INSERT INTO workouts (workout_id, name, trainer_id, room_id, datetime, max_participants, current_participants, description, status) VALUES
-- Сегодня
('workout-001', 'Утренняя йога', '550e8400-e29b-41d4-a716-446655440102', 'room-004', CURRENT_DATE + INTERVAL '9 hours', 12, 8, 'Расслабляющая утренняя практика', 'active'),
('workout-002', 'Силовая тренировка', '550e8400-e29b-41d4-a716-446655440101', 'room-001', CURRENT_DATE + INTERVAL '18 hours', 15, 12, 'Проработка всех групп мышц', 'active'),
('workout-003', 'Кардио-интенсив', 'trainer-004', 'room-002', CURRENT_DATE + INTERVAL '20 hours', 15, 10, 'Интервальная тренировка', 'active'),

-- Завтра
('workout-004', 'Кроссфит', '550e8400-e29b-41d4-a716-446655440103', 'room-003', CURRENT_DATE + INTERVAL '1 day' + INTERVAL '10 hours', 10, 6, 'Высокоинтенсивная тренировка', 'active'),
('workout-005', 'Аквааэробика', 'trainer-005', 'room-005', CURRENT_DATE + INTERVAL '1 day' + INTERVAL '19 hours', 8, 5, 'Тренировка в воде', 'active'),

-- Послезавтра
('workout-006', 'Йога для начинающих', '550e8400-e29b-41d4-a716-446655440102', 'room-004', CURRENT_DATE + INTERVAL '2 days' + INTERVAL '11 hours', 12, 4, 'Базовые асаны и дыхание', 'active'),
('workout-007', 'Функциональный тренинг', '550e8400-e29b-41d4-a716-446655440101', 'room-001', CURRENT_DATE + INTERVAL '2 days' + INTERVAL '17 hours', 15, 9, 'Улучшение координации и силы', 'active'),

-- Через 3 дня (отмененная тренировка)
('workout-008', 'Вечерний стретчинг', 'trainer-004', 'room-002', CURRENT_DATE + INTERVAL '3 days' + INTERVAL '20 hours', 15, 2, 'Растяжка после рабочего дня', 'cancelled')
ON CONFLICT (workout_id) DO NOTHING;