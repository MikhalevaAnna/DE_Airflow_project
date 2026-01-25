-- owner: admin
-- schedule: @once
-- tags: [setup, database]
-- description: Initial database setup

DROP TABLE IF EXISTS users_my;
CREATE TABLE users_my (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP INDEX IF EXISTS  idx_users_my_email;
CREATE INDEX idx_users_my_email ON users_my(email);

INSERT INTO users_my (username, email)
VALUES ('admin', 'admin@example.com');
