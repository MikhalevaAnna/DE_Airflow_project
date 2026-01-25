-- setup_database.sql
-- DAG_SCHEDULE_INTERVAL: @once
-- DAG_TAGS: ['setup', 'ddl']
-- DAG_CATCHUP: false

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP INDEX IF EXISTS idx_users_email;

CREATE INDEX idx_users_email ON users(email);

DROP INDEX IF EXISTS idx_orders_user_id;

CREATE INDEX idx_orders_user_id ON orders(user_id);

DELETE FROM orders;

DELETE FROM users;

INSERT INTO users (username, email) VALUES
('john_doe', 'john@example.com'),
('jane_smith', 'jane@example.com');

UPDATE users SET email = 'john.doe@example.com' WHERE username = 'john_doe';

SELECT email FROM users AS u INNER JOIN orders AS o
ON u.id = o.user_id
WHERE u.email = 'john.doe@example.com';


SELECT user_id FROM  orders
WHERE amount = (SELECT MAX(amount) FROM orders);

WITH SubTotal AS ( SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id )
SELECT user_id, total FROM SubTotal;


-- setup_database.sql
-- DAG_SCHEDULE_INTERVAL: @daily
-- DAG_TAGS: ['cte', 'analytics']
-- DAG_CATCHUP: false

-- 1. Создание таблицы users (если не существует)
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Создание таблицы orders (если не существует)
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount DECIMAL(10, 2) NOT NULL CHECK (amount > 0),
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Создание таблицы employees (если не существует) - для рекурсивного CTE
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(50),
    manager_id INTEGER REFERENCES employees(id) ON DELETE SET NULL,
    department VARCHAR(50),
    salary DECIMAL(10, 2),
    hired_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Создание индексов для оптимизации запросов
DROP INDEX IF EXISTS idx_orders_user_id;
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
DROP INDEX IF EXISTS idx_orders_order_date;
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
DROP INDEX IF EXISTS idx_employees_manager_id;
CREATE INDEX IF NOT EXISTS idx_employees_manager_id ON employees(manager_id);
DROP INDEX IF EXISTS idx_users_email;
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- 5. Вставка тестовых данных в таблицу users (если нет данных)
INSERT INTO users (username, email)
SELECT 'john_doe', 'john@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'john_doe');

INSERT INTO users (username, email)
SELECT 'jane_smith', 'jane@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'jane_smith');

INSERT INTO users (username, email)
SELECT 'mike_jones', 'mike@example.com'
WHERE NOT EXISTS (SELECT 1 FROM users WHERE username = 'mike_jones');

-- 6. Вставка тестовых данных в таблицу orders (если нет данных)
INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'john_doe'),
    100.50,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'john_doe') AND amount = 100.50);

INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'john_doe'),
    75.25,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'john_doe') AND amount = 75.25);

INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'jane_smith'),
    200.00,
    'completed'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'jane_smith') AND amount = 200.00);

INSERT INTO orders (user_id, amount, status)
SELECT
    (SELECT id FROM users WHERE username = 'mike_jones'),
    50.00,
    'pending'
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = (SELECT id FROM users WHERE username = 'mike_jones') AND amount = 50.00);

-- 7. Вставка тестовых данных в таблицу employees (иерархическая структура)
INSERT INTO employees (name, position, manager_id, department, salary)
SELECT 'Alice Johnson', 'CEO', NULL, 'Executive', 150000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Alice Johnson');

INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Bob Smith',
    'CTO',
    (SELECT id FROM employees WHERE name = 'Alice Johnson'),
    'Executive',
    120000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Bob Smith');

INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Carol Davis',
    'CFO',
    (SELECT id FROM employees WHERE name = 'Alice Johnson'),
    'Executive',
    110000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Carol Davis');

INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'David Wilson',
    'Engineering Manager',
    (SELECT id FROM employees WHERE name = 'Bob Smith'),
    'Engineering',
    90000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'David Wilson');

INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Eva Brown',
    'Senior Developer',
    (SELECT id FROM employees WHERE name = 'David Wilson'),
    'Engineering',
    80000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Eva Brown');

INSERT INTO employees (name, position, manager_id, department, salary)
SELECT
    'Frank Miller',
    'Junior Developer',
    (SELECT id FROM employees WHERE name = 'David Wilson'),
    'Engineering',
    60000.00
WHERE NOT EXISTS (SELECT 1 FROM employees WHERE name = 'Frank Miller');

-- 8. CTE с SELECT - статистика по пользователям
WITH user_stats AS (
    SELECT
        user_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_order_amount,
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
)
SELECT
    u.username,
    u.email,
    us.order_count,
    us.total_amount,
    us.avg_order_amount,
    us.first_order_date,
    us.last_order_date
FROM users u
LEFT JOIN user_stats us ON u.id = us.user_id
ORDER BY us.total_amount DESC NULLS LAST;

-- 9. Рекурсивный CTE - иерархия сотрудников
WITH RECURSIVE employee_hierarchy AS (
    -- Базовый случай: сотрудники без менеджера (верхний уровень)
    SELECT
        id,
        name,
        position,
        manager_id,
        department,
        salary,
        1 as level,
        ARRAY[id] as path,
        name::TEXT as hierarchy_path
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Рекурсивный случай: подчиненные
    SELECT
        e.id,
        e.name,
        e.position,
        e.manager_id,
        e.department,
        e.salary,
        eh.level + 1 as level,
        eh.path || e.id as path,
        eh.hierarchy_path || ' -> ' || e.name as hierarchy_path
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT
    level,
    name,
    position,
    department,
    salary,
    hierarchy_path
FROM employee_hierarchy
ORDER BY path;

-- 10. CTE с INSERT - добавление новых пользователей
WITH new_users AS (
    SELECT
        'alice_wonder' as username,
        'alice.wonder@example.com' as email
    UNION ALL
    SELECT
        'bob_marley' as username,
        'bob.marley@example.com' as email
    UNION ALL
    SELECT
        'charlie_chaplin' as username,
        'charlie.chaplin@example.com' as email
)
INSERT INTO users (username, email)
SELECT username, email
FROM new_users nu
WHERE NOT EXISTS (
    SELECT 1 FROM users u WHERE u.username = nu.username OR u.email = nu.email
)
RETURNING id, username, email, created_at;

-- 11. Дополнительный CTE - агрегация по отделам
WITH department_stats AS (
    SELECT
        department,
        COUNT(*) as employee_count,
        ROUND(AVG(salary), 2) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        SUM(salary) as total_salary_budget
    FROM employees
    WHERE department IS NOT NULL
    GROUP BY department
)
SELECT
    ds.department,
    ds.employee_count,
    ds.avg_salary,
    ds.min_salary,
    ds.max_salary,
    ds.total_salary_budget,
    ROUND(ds.total_salary_budget / SUM(ds.total_salary_budget) OVER () * 100, 2) as budget_percentage
FROM department_stats ds
ORDER BY ds.total_salary_budget DESC;

-- 12. Обновление данных через CTE
WITH user_orders_summary AS (
    SELECT
        u.id as user_id,
        u.username,
        COUNT(o.order_id) as total_orders,
        COALESCE(SUM(o.amount), 0) as lifetime_value,
        MAX(o.order_date) as last_purchase_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id AND o.status = 'completed'
    GROUP BY u.id, u.username
)
UPDATE users u
SET
    email = CASE
        WHEN uos.lifetime_value > 1000 THEN REPLACE(u.email, '@example.com', '@vip.example.com')
        ELSE u.email
    END
FROM user_orders_summary uos
WHERE u.id = uos.user_id
AND uos.lifetime_value > 1000
RETURNING u.id, u.username, u.email, uos.lifetime_value;

