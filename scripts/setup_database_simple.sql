-- DAG: setup_database_simple
-- schedule: @once
-- owner: dba
-- tags: [setup, database]
-- description: Настройка базы данных

-- Создание таблиц
DROP TABLE IF EXISTS users_main;
CREATE TABLE IF NOT EXISTS users_main (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS orders_main;
CREATE TABLE IF NOT EXISTS orders_main (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id),
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индексов
CREATE INDEX idx_orders_main_user_id ON orders_main(user_id);
CREATE INDEX idx_orders_main_created_at ON orders_main(created_at);

-- Начальные данные
INSERT INTO users_main (username) VALUES ('admin'), ('user1');