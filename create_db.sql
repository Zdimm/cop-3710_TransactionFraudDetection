-- Drop tables 
BEGIN
    FOR t IN (SELECT table_name FROM user_tables 
              WHERE table_name IN (
                'TRANSACTIONS','CARDHOLDER_LOCATION','CARDHOLDER',
                'MERCHANT','MERCHANT_CATEGORY','CITY_DETAILS',
                'STATES','OCCUPATION'
              ))
    LOOP
        EXECUTE IMMEDIATE 'DROP TABLE ' || t.table_name || ' CASCADE CONSTRAINTS';
    END LOOP;
END;
/

CREATE TABLE occupation (
    occupation_id NUMBER PRIMARY KEY,
    job VARCHAR2(100) NOT NULL
);

CREATE TABLE cardholder (
    cardholder_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50) NOT NULL,
    last_name VARCHAR2(50) NOT NULL,
    dob DATE NOT NULL,
    occupation_id NUMBER NOT NULL,
    gender VARCHAR2(10) NOT NULL
        CHECK (gender IN ('Male','Female','Nonbinary','Other')),
    CONSTRAINT fk_cardholder_occupation
        FOREIGN KEY (occupation_id) REFERENCES occupation(occupation_id)
);

CREATE TABLE states (
    state_code CHAR(2) PRIMARY KEY,
    state_name VARCHAR2(50) NOT NULL
);

CREATE TABLE city_details (
    city_id NUMBER PRIMARY KEY,
    city VARCHAR2(50) NOT NULL,
    state_code CHAR(2) NOT NULL,
    zip_code VARCHAR2(10) NOT NULL,
    city_pop NUMBER(8,0) NOT NULL,
    CONSTRAINT fk_city_state
        FOREIGN KEY (state_code) REFERENCES states(state_code)
);

CREATE TABLE cardholder_location (
    location_id NUMBER PRIMARY KEY,
    cardholder_id NUMBER,
    street VARCHAR2(50) NOT NULL,
    city_id NUMBER NOT NULL,
    latitude NUMBER(10,8) NOT NULL,
    longitude NUMBER(11,8) NOT NULL,
    CONSTRAINT fk_location_cardholder
        FOREIGN KEY (cardholder_id) REFERENCES cardholder(cardholder_id),
    CONSTRAINT fk_location_city
        FOREIGN KEY (city_id) REFERENCES city_details(city_id)
);

CREATE TABLE merchant_category (
    merchant_cat_id NUMBER PRIMARY KEY,
    category_name VARCHAR2(32) NOT NULL
);

CREATE TABLE merchant (
    merchant_id NUMBER PRIMARY KEY,
    merchant_name VARCHAR2(50) NOT NULL,
    merchant_cat_id NUMBER NOT NULL,
    merchant_lat NUMBER(10,8) NOT NULL,
    merchant_long NUMBER(11,8) NOT NULL,
    CONSTRAINT fk_merchant_category
        FOREIGN KEY (merchant_cat_id) REFERENCES merchant_category(merchant_cat_id)
);

CREATE TABLE transactions (
    transaction_id NUMBER PRIMARY KEY,
    cardholder_id NUMBER NOT NULL,
    merchant_id NUMBER NOT NULL,
    transaction_date DATE NOT NULL,
    amount NUMBER(6,2) CHECK (amount >= 0),
    unix_time NUMBER NOT NULL,
    is_fraud NUMBER(1) CHECK (is_fraud IN (0, 1)),
    CONSTRAINT fk_txn_cardholder
        FOREIGN KEY (cardholder_id) REFERENCES cardholder(cardholder_id),
    CONSTRAINT fk_txn_merchant
        FOREIGN KEY (merchant_id)   REFERENCES merchant(merchant_id)
);
