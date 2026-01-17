BEGIN;

-- Инициализация базы данных User Service
CREATE DATABASE user_db;
\c user_db;
-- Таблица типов пользователей
CREATE TABLE IF NOT EXISTS user_type (
    type_id INTEGER PRIMARY KEY,
    name VARCHAR(20) NOT NULL
);
-- Таблица пользователей
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(36) PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    type_id INTEGER NOT NULL,
    subscription_end DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active',
    FOREIGN KEY (type_id) REFERENCES user_type(type_id)
);

-- Вставляем типы пользователей
INSERT INTO user_type (type_id, name) VALUES
(1, 'client'),
(2, 'trainer'),
(3, 'admin')
ON CONFLICT (type_id) DO NOTHING;

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


END;