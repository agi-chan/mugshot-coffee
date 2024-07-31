DROP DATABASE IF EXISTS mugshot_coffee;

CREATE DATABASE mugshot_coffee;

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price FLOAT NOT NULL
);
CREATE TABLE transactions(
    transaction_id serial NOT NULL primary key,
    date VARCHAR(255) NOT NULL,
    time VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    total_cost FLOAT NOT NULL,
    payment_method VARCHAR(4) NOT NULL
);
CREATE TABLE order_items(
transaction_id INT NOT NULL,
product_id INT NOT NULL ,
product_quantity INT NOT NULL,
PRIMARY KEY(transaction_id,product_id),
FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
FOREIGN KEY (product_id) REFERENCES products(product_id)
);