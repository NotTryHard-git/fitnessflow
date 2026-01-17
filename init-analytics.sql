-- Инициализация базы данных Analytics Service
DROP DATABASE analytics_db;
CREATE DATABASE analytics_db;
\c analytics_db;

-- Таблица месячной статистики
CREATE TABLE IF NOT EXISTS monthly_statistics (
    id SERIAL PRIMARY KEY,
    month_year VARCHAR(7) NOT NULL,
    total_bookings INTEGER DEFAULT 0,
    unique_users INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(month_year)
);

-- Вставляем месячную статистику за последние 3 месяца
INSERT INTO monthly_statistics (month_year, total_bookings, unique_users) VALUES
(TO_CHAR(CURRENT_DATE - INTERVAL '2 months', 'YYYY-MM'), 42, 28),
(TO_CHAR(CURRENT_DATE - INTERVAL '1 month', 'YYYY-MM'), 58, 35),
(TO_CHAR(CURRENT_DATE, 'YYYY-MM'), 12, 8)
ON CONFLICT (month_year) DO NOTHING;


