-- schedule: 0 1 * * *
-- owner: analytics
-- tags: [reporting, analytics, sales]
-- description: Ежедневный отчет по продажам
-- retries: 2
-- retry_delay: 300

-- Создаем таблицу заказов (orders)
DROP TABLE IF EXISTS orders_info;
CREATE TABLE IF NOT EXISTS orders_info (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу для ежедневных отчетов (daily_reports)
DROP TABLE IF EXISTS daily_reports;
CREATE TABLE IF NOT EXISTS daily_reports (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL UNIQUE,
    total_orders INT NOT NULL,
    total_revenue DECIMAL(12,2) NOT NULL,
    average_order_value DECIMAL(10,2),
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставляем тестовые данные за последние 3 дня
-- Вчерашние заказы (для отчета)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(1, 100.50, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '10:30'),
(2, 75.25, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '14:15'),
(3, 200.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '16:45'),
(1, 50.00, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '20:10'),
(4, 300.75, CURRENT_DATE - INTERVAL '1 day' + INTERVAL '22:30');

-- Позавчерашние заказы (для проверки where условия)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(5, 125.00, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '09:00'),
(2, 80.50, CURRENT_DATE - INTERVAL '2 days' + INTERVAL '15:20');

-- Сегодняшние заказы (будут в следующем отчете)
INSERT INTO orders_info (user_id, amount, created_at) VALUES
(3, 150.00, CURRENT_DATE + INTERVAL '8 hours'),
(6, 95.30, CURRENT_DATE + INTERVAL '12 hours');

-- Создаем индекс для ускорения запросов по дате
CREATE INDEX IF NOT EXISTS idx_orders_info_created_at ON orders_info(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_info_user_created ON orders_info(user_id, created_at);

-- Основной запрос для генерации ежедневного отчета
WITH daily_stats AS (
    SELECT
        DATE(created_at) as report_date,
        COUNT(*) as total_orders,
        SUM(amount) as total_revenue
    FROM orders_info
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    GROUP BY DATE(created_at)
)
INSERT INTO daily_reports (report_date, total_orders, total_revenue)
SELECT
    report_date,
    total_orders,
    total_revenue
FROM daily_stats
ON CONFLICT (report_date)
DO UPDATE SET
    total_orders = EXCLUDED.total_orders,
    total_revenue = EXCLUDED.total_revenue,
    generated_at = CURRENT_TIMESTAMP;

-- Дополнительный запрос для обновления средней стоимости заказа
UPDATE daily_reports
SET average_order_value = total_revenue / NULLIF(total_orders, 0)
WHERE report_date = CURRENT_DATE - INTERVAL '1 day';

