-- Инициализация базы данных Attendance Service
CREATE DATABASE attendance_db;
\c attendance_db;

-- Таблица уведомлений
CREATE TABLE IF NOT EXISTS notifications (
    notification_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    type VARCHAR(50) NOT NULL, -- SUCCESSFUL BOOKING, CANCELLED BOOKING
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    related_booking_id VARCHAR(36),
    related_workout_id VARCHAR(36)
);

-- Таблица посещений
CREATE TABLE IF NOT EXISTS attendance (
    attendance_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    workout_id VARCHAR(36) NOT NULL,
    booking_id VARCHAR(36),
    attended BOOLEAN DEFAULT FALSE,
    checked_in_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, workout_id)
);

-- Вставляем уведомления
INSERT INTO notifications (notification_id, user_id, type, title, message, related_booking_id, related_workout_id) VALUES
-- Уведомления о успешном бронировании
('notif-001', '550e8400-e29b-41d4-a716-446655440001', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Утренняя йога"', 'booking-001', 'workout-001'),
('notif-002', '550e8400-e29b-41d4-a716-446655440002', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Утренняя йога"', 'booking-002', 'workout-001'),
('notif-003', '550e8400-e29b-41d4-a716-446655440003', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Силовая тренировка"', 'booking-004', 'workout-002'),
('notif-004', '550e8400-e29b-41d4-a716-446655440005', 'SUCCESSFUL_BOOKING', 'Бронирование подтверждено', 'Вы успешно записались на тренировку "Аквааэробика"', 'booking-008', 'workout-005'),

-- Уведомления об отмене бронирования
('notif-005', '550e8400-e29b-41d4-a716-446655440004', 'CANCELLED_BOOKING', 'Бронирование отменено', 'Ваше бронирование на тренировку "Утренняя йога" было отменено', 'booking-009', 'workout-001'),
('notif-006', '550e8400-e29b-41d4-a716-446655440002', 'CANCELLED_BOOKING', 'Тренировка отменена', 'Тренировка "Вечерний стретчинг" была отменена администратором', 'booking-010', 'workout-008')
ON CONFLICT (notification_id) DO NOTHING;

-- Вставляем посещения (для прошедших тренировок)
INSERT INTO attendance (attendance_id, user_id, workout_id, booking_id, attended, checked_in_at) VALUES
-- Посещенные тренировки (вчерашняя йога)
('attend-001', '550e8400-e29b-41d4-a716-446655440001', 'workout-001', 'booking-001', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 5 minutes'),
('attend-002', '550e8400-e29b-41d4-a716-446655440002', 'workout-001', 'booking-002', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 2 minutes'),
('attend-003', '550e8400-e29b-41d4-a716-446655440003', 'workout-001', 'booking-003', true, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '9 hours 10 minutes'),

-- Неявки (силовая тренировка 2 дня назад)
('attend-004', '550e8400-e29b-41d4-a716-446655440001', 'workout-002', 'booking-004', false, NULL),
('attend-005', '550e8400-e29b-41d4-a716-446655440005', 'workout-002', 'booking-005', false, NULL),

-- Посещенные кардио тренировки
('attend-006', '550e8400-e29b-41d4-a716-446655440003', 'workout-003', 'booking-006', true, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '20 hours 3 minutes')
ON CONFLICT (attendance_id) DO NOTHING;
