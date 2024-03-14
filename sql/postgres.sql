-- CREATE DATABASE
CREATE DATABASE my_database;

-- CREATE TABLES
CREATE TABLE Province (
    province_id SERIAL PRIMARY KEY,
    province_name VARCHAR(255)
);

CREATE TABLE District (
    district_id SERIAL PRIMARY KEY,
    province_id INT,
    district_name VARCHAR(255),
    FOREIGN KEY (province_id) REFERENCES Province(province_id)
);

CREATE TABLE Case (
    Id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) CHECK (status_name IN ('suspect', 'closecontact', 'probable', 'confirmation')),
    status_detail VARCHAR(255)
);

CREATE TABLE Province_Daily (
    Id SERIAL PRIMARY KEY,
    province_id INT,
    case_id INT,
    date DATE,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE Province_Monthly (
    Id SERIAL PRIMARY KEY,
    province_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE Province_Yearly (
    Id SERIAL PRIMARY KEY,
    province_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE District_Monthly (
    Id SERIAL PRIMARY KEY,
    district_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES District(district_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE District_Yearly (
    Id SERIAL PRIMARY KEY,
    district_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES District(district_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);
