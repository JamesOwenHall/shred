DROP DATABASE IF EXISTS shred_test;
CREATE DATABASE shred_test;

CREATE TABLE shred_test.users (
    user_id BIGINT,
    email VARCHAR(64),
    passhash VARCHAR(64),
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    PRIMARY KEY (user_id)
);

INSERT INTO shred_test.users (user_id, email, passhash, first_name, last_name)
VALUES
(1, 'john.smith@example.com', 'password', 'John', 'Smith'),
(2, 'jane.smith@example.com', 'password', 'Jane', 'Smith');

CREATE TABLE shred_test.orders (
    order_id BIGINT,
    user_id BIGINT,
    subtotal_price BIGINT,
    taxes BIGINT,
    total_price BIGINT,
    PRIMARY KEY (user_id, order_id)
);

INSERT INTO shred_test.orders (order_id, user_id, subtotal_price, taxes, total_price)
VALUES
(1, 1, 23, 2, 25),
(2, 1, 50, 5, 55),
(3, 2, 10, 1, 11);
