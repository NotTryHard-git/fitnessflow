-- Инициализация базы данных Booking Service
DROP DATABASE booking_db;
CREATE DATABASE booking_db;
\c booking_db;

-- Таблица бронирований
CREATE TABLE IF NOT EXISTS bookings (
    booking_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    workout_id VARCHAR(36) NOT NULL,
    status VARCHAR(20) NOT NULL, --PENDING, CONFIRM, CANCEL
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставляем бронирования
INSERT INTO bookings (booking_id, user_id, workout_id, status, created_at) VALUES
-- Подтвержденные бронирования
('booking-001', '550e8400-e29b-41d4-a716-446655440001', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '2 days'),
('booking-002', '550e8400-e29b-41d4-a716-446655440002', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '1 day'),
('booking-003', '550e8400-e29b-41d4-a716-446655440003', 'workout-001', 'CONFIRMED', CURRENT_DATE - INTERVAL '3 days'),
('booking-004', '550e8400-e29b-41d4-a716-446655440001', 'workout-002', 'CONFIRMED', CURRENT_DATE - INTERVAL '5 days'),
('booking-005', '550e8400-e29b-41d4-a716-446655440005', 'workout-002', 'CONFIRMED', CURRENT_DATE - INTERVAL '4 days'),
('booking-006', '550e8400-e29b-41d4-a716-446655440003', 'workout-003', 'CONFIRMED', CURRENT_DATE - INTERVAL '2 days'),
('booking-007', '550e8400-e29b-41d4-a716-446655440002', 'workout-004', 'CONFIRMED', CURRENT_DATE - INTERVAL '1 day'),
('booking-008', '550e8400-e29b-41d4-a716-446655440001', 'workout-005', 'CONFIRMED', CURRENT_DATE),

-- Отмененные бронирования
('booking-009', '550e8400-e29b-41d4-a716-446655440004', 'workout-001', 'CANCELLED', CURRENT_DATE - INTERVAL '3 days'),
('booking-010', '550e8400-e29b-41d4-a716-446655440002', 'workout-008', 'CANCELLED', CURRENT_DATE - INTERVAL '2 days'),

-- Ожидающие подтверждения
('booking-011', '550e8400-e29b-41d4-a716-446655440005', 'workout-006', 'PENDING', CURRENT_DATE - INTERVAL '1 day'),
('booking-012', '550e8400-e29b-41d4-a716-446655440003', 'workout-007', 'PENDING', CURRENT_DATE)
ON CONFLICT (booking_id) DO NOTHING;

