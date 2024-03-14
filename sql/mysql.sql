-- CREATE DATABASE 
CREATE DATABASE IF NOT EXISTS my_database;
USE my_database;

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS Province (
    province_id INT AUTO_INCREMENT PRIMARY KEY,
    province_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS District (
    district_id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    district_name VARCHAR(255),
    FOREIGN KEY (province_id) REFERENCES Province(province_id)
);

CREATE TABLE IF NOT EXISTS Case (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    status_name ENUM('suspect', 'closecontact', 'probable', 'confirmation'),
    status_detail VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Province_Daily (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    date DATE,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE IF NOT EXISTS Province_Monthly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE IF NOT EXISTS Province_Yearly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES Province(province_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE IF NOT EXISTS District_Monthly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    district_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES District(district_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

CREATE TABLE IF NOT EXISTS District_Yearly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    district_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES District(district_id),
    FOREIGN KEY (case_id) REFERENCES Case(Id)
);

