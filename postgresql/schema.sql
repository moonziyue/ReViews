CREATE TABLE customer (
	customer_id varchar(50) PRIMARY KEY,
	one_star integer DEFAULT 0,
	two_star integer DEFAULT 0,
	three_star integer DEFAULT 0,
	four_star integer DEFAULT 0,
	five_star integer DEFAULT 0,
	total_purchase integer DEFAULT 0,
	verified_purchase integer DEFAULT 0,
	helpful_votes integer DEFAULT 0
);

CREATE TABLE product (
	product_id varchar(50) PRIMARY KEY,
	review_date DATE NOT NULL,
	avg_rating double precision NOT NULL,
	total_reviews integer NOT NULL
);