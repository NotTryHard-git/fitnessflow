BEGIN;

DROP DATABASE user_db;
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
    password VARCHAR(36),
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




END;