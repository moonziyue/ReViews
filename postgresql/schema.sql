CREATE TABLE customer (
	customer_id varchar(15) PRIMARY KEY,
	one_star integer DEFAULT 0,
	two_star integer DEFAULT 0,
	three_star integer DEFAULT 0,
	four_star integer DEFAULT 0,
	five_star integer DEFAULT 0,
	count integer DEFAULT 0,
	verified_purchase boolean DEFAULT FALSE,
	helpful_votes integer DEFAULT 0
);

CREATE TABLE product (
	product_id varchar(15) PRIMARY KEY,
	review_date DATE NOT NULL,
	avg_rating integer NOT NULL
);