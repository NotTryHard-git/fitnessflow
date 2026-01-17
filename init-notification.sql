-- Инициализация базы данных Attendance Service
DROP DATABASE attendance_db;
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
