-- DAG_SCHEDULE_INTERVAL: @daily
-- DAG_TAGS: ['setup', 'ddl', 'dml']
-- DAG_OWNER: data_team
-- DAG_RETRIES: 2
-- DAG_RETRY_DELAY: 10

-- Только если нужно пересоздать все с нуля
-- =============================================
-- TASK 1: Drop existing tables (full cleanup)
-- =============================================
-- dependencies: none (starts the chain)
DROP TABLE IF EXISTS sales_by_category CASCADE;
DROP TABLE IF EXISTS demand_forecast CASCADE;
DROP TABLE IF EXISTS customer_analytics CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- =============================================
-- TASK 2: Create customers table
-- =============================================
-- dependencies: drop_tables >> create_customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    phone VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activity_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- =============================================
-- TASK 3: Create products table
-- =============================================
-- dependencies: drop_tables >> create_products
-- (can run in parallel with create_customers)
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100),
    supplier_id INTEGER,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
    stock_quantity INTEGER DEFAULT 0 CHECK (stock_quantity >= 0),
    reorder_level INTEGER DEFAULT 10,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TASK 4: Create transactions table
-- =============================================
-- dependencies: create_customers >> create_transactions
-- (must wait for customers table)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    amount DECIMAL(12,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) DEFAULT 'USD',
    transaction_type VARCHAR(20) CHECK (transaction_type IN ('purchase', 'refund', 'deposit', 'withdrawal')),
    status VARCHAR(20) DEFAULT 'completed',
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    description TEXT
);

-- =============================================
-- TASK 5: Create indexes for customers
-- =============================================
-- dependencies: create_customers >> create_customer_indexes
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_last_name ON customers(last_name);

-- =============================================
-- TASK 6: Create indexes for transactions
-- =============================================
-- dependencies: create_transactions >> create_transaction_indexes
CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_type_status ON transactions(transaction_type, status);

-- =============================================
-- TASK 7: Create indexes for products
-- =============================================
-- dependencies: create_products >> create_product_indexes
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_supplier ON products(supplier_id);
-- ЗАДАЧА 8: TRUNCATE таблиц (если нужна очистка перед вставкой)
TRUNCATE TABLE transactions, products, customers CASCADE;

ALTER SEQUENCE IF EXISTS customers_customer_id_seq RESTART WITH 1;
INSERT INTO customers (first_name, last_name, email, phone) VALUES
('Иван', 'Иванов', 'ivan.ivanov@example.com', '+79161234567'),
('Мария', 'Петрова', 'maria.petrova@example.com', '+79162345678'),
('Алексей', 'Сидоров', 'alexey.sidorov@example.com', '+79163456789'),
('Елена', 'Кузнецова', 'elena.kuznetsova@example.com', '+79164567890'),
('Дмитрий', 'Смирнов', 'dmitry.smirnov@example.com', '+79165678901')
ON CONFLICT (email) DO NOTHING;


ALTER SEQUENCE IF EXISTS products_product_id_seq RESTART WITH 1;
INSERT INTO products (product_name, category, price, cost, stock_quantity) VALUES
('Ноутбук Dell XPS 13', 'Электроника', 129999.99, 95000.00, 25),
('Смартфон iPhone 15', 'Электроника', 89999.99, 65000.00, 50),
('Кофеварка DeLonghi', 'Бытовая техника', 24999.99, 18000.00, 30),
('Футболка мужская', 'Одежда', 1999.99, 800.00, 100),
('Книга "Python для анализа данных"', 'Книги', 2999.99, 1500.00, 45),
('Наушники Sony WH-1000XM5', 'Электроника', 34999.99, 22000.00, 20),
('Чайник электрический', 'Бытовая техника', 4999.99, 3000.00, 60)
ON CONFLICT DO NOTHING;


ALTER SEQUENCE IF EXISTS transactions_transaction_id_seq RESTART WITH 1;
INSERT INTO transactions (customer_id, amount, transaction_type, description)
SELECT
    (SELECT customer_id FROM customers WHERE email = 'ivan.ivanov@example.com'),
    129999.99,
    'purchase',
    'Покупка ноутбука Dell XPS 13'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'ivan.ivanov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'maria.petrova@example.com'),
    89999.99,
    'purchase',
    'Покупка iPhone 15'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'maria.petrova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'alexey.sidorov@example.com'),
    24999.99,
    'purchase',
    'Покупка кофеварки DeLonghi'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'alexey.sidorov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'ivan.ivanov@example.com'),
    1999.99,
    'purchase',
    'Покупка футболки'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'ivan.ivanov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'elena.kuznetsova@example.com'),
    2999.99,
    'purchase',
    'Покупка книги по Python'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'elena.kuznetsova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'maria.petrova@example.com'),
    34999.99,
    'purchase',
    'Покупка наушников Sony'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'maria.petrova@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'dmitry.smirnov@example.com'),
    4999.99,
    'purchase',
    'Покупка электрического чайника'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'dmitry.smirnov@example.com')
UNION ALL
SELECT
    (SELECT customer_id FROM customers WHERE email = 'alexey.sidorov@example.com'),
    1999.99,
    'refund',
    'Возврат футболки'
WHERE EXISTS (SELECT 1 FROM customers WHERE email = 'alexey.sidorov@example.com')
ON CONFLICT DO NOTHING;
-- analytics_etl.sql
-- DAG_SCHEDULE_INTERVAL: @hourly
-- DAG_TAGS: ['analytics', 'etl', 'reporting']
-- DAG_OWNER: analytics_team
-- DAG_RETRIES: 3
-- DAG_RETRY_DELAY: 5

-- 1. Создание витрины данных клиентов
CREATE TABLE IF NOT EXISTS customer_analytics (
    customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
    total_transactions INTEGER DEFAULT 0,
    total_spent DECIMAL(15,2) DEFAULT 0,
    avg_transaction_amount DECIMAL(10,2) DEFAULT 0,
    first_transaction_date TIMESTAMP,
    last_transaction_date TIMESTAMP,
    favorite_category VARCHAR(100),
    days_since_last_activity INTEGER,
    customer_segment VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Создание витрины продаж по категориям
CREATE TABLE IF NOT EXISTS sales_by_category (
    category VARCHAR(100),
    period_date DATE,
    total_sales DECIMAL(15,2) DEFAULT 0,
    total_quantity INTEGER DEFAULT 0,
    unique_customers INTEGER DEFAULT 0,
    avg_sale_amount DECIMAL(10,2) DEFAULT 0,
    PRIMARY KEY (category, period_date)
);

-- 3. Создание таблицы для прогнозов спроса
CREATE TABLE IF NOT EXISTS demand_forecast (
    product_id INTEGER REFERENCES products(product_id),
    forecast_date DATE,
    predicted_demand INTEGER,
    confidence_interval_lower INTEGER,
    confidence_interval_upper INTEGER,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, forecast_date)
);

-- 4. Ежедневное обновление аналитики клиентов
DELETE FROM customer_analytics;
WITH customer_stats AS (
    SELECT
        c.customer_id,
        COUNT(t.transaction_id) as transaction_count,
        SUM(t.amount) as total_amount,
        AVG(t.amount) as avg_amount,
        MIN(t.transaction_date) as first_purchase,
        MAX(t.transaction_date) as last_purchase,
        MODE() WITHIN GROUP (ORDER BY p.category) as top_category
    FROM customers c
    LEFT JOIN transactions t ON c.customer_id = t.customer_id
    LEFT JOIN products p ON 1=1  -- Здесь должна быть реальная связь
    WHERE t.status = 'completed'
    GROUP BY c.customer_id
)
INSERT INTO customer_analytics (
    customer_id,
    total_transactions,
    total_spent,
    avg_transaction_amount,
    first_transaction_date,
    last_transaction_date,
    favorite_category,
    days_since_last_activity,
    customer_segment
)
SELECT
    cs.customer_id,
    cs.transaction_count,
    cs.total_amount,
    cs.avg_amount,
    cs.first_purchase,
    cs.last_purchase,
    cs.top_category,
    (CURRENT_DATE - cs.last_purchase::DATE) as days_since_last,
    CASE
        WHEN cs.total_amount > 100000 THEN 'VIP'
        WHEN cs.total_amount > 50000 THEN 'Premium'
        WHEN cs.total_amount > 10000 THEN 'Regular'
        ELSE 'New'
    END as segment
FROM customer_stats cs
ON CONFLICT (customer_id) DO UPDATE SET
    total_transactions = EXCLUDED.total_transactions,
    total_spent = EXCLUDED.total_spent,
    avg_transaction_amount = EXCLUDED.avg_transaction_amount,
    last_transaction_date = EXCLUDED.last_transaction_date,
    favorite_category = EXCLUDED.favorite_category,
    days_since_last_activity = EXCLUDED.days_since_last_activity,
    customer_segment = EXCLUDED.customer_segment,
    updated_at = CURRENT_TIMESTAMP;

-- 5. Ежедневная агрегация продаж по категориям
DELETE FROM sales_by_category;
WITH daily_sales AS (
    SELECT
        p.category,
        DATE(t.transaction_date) as sale_date,
        SUM(t.amount) as daily_total,
        COUNT(t.transaction_id) as daily_count,
        COUNT(DISTINCT t.customer_id) as customer_count
    FROM transactions t
    JOIN products p ON 1=1  -- Здесь должна быть реальная связь
    WHERE t.transaction_type = 'purchase'
        AND t.status = 'completed'
        AND DATE(t.transaction_date) = CURRENT_DATE
    GROUP BY p.category, DATE(t.transaction_date)
)
INSERT INTO sales_by_category (
    category,
    period_date,
    total_sales,
    total_quantity,
    unique_customers,
    avg_sale_amount
)
SELECT
    category,
    sale_date,
    daily_total,
    daily_count,
    customer_count,
    CASE
        WHEN daily_count > 0 THEN daily_total / daily_count
        ELSE 0
    END as avg_sale
FROM daily_sales
ON CONFLICT (category, period_date) DO UPDATE SET
    total_sales = EXCLUDED.total_sales,
    total_quantity = EXCLUDED.total_quantity,
    unique_customers = EXCLUDED.unique_customers,
    avg_sale_amount = EXCLUDED.avg_sale_amount;