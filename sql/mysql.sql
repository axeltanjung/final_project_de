-- CREATE DATABASE 
CREATE DATABASE IF NOT EXISTS staging_area;
USE staging_area;

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS staging_table (
    CLOSECONTACT INT,
    CONFIRMATION INT,
    PROBABLE INT,
    SUSPECT INT,
    closecontact_dikarantina INT,
    closecontact_discarded INT,
    closecontact_meninggal INT,
    confirmation_meninggal INT,
    confirmation_sembuh INT,
    kode_kab VARCHAR(255),
    kode_prov VARCHAR(255),
    nama_kab VARCHAR(255),
    nama_prov VARCHAR(255),
    probable_diisolasi INT,
    probable_discarded INT,
    probable_meninggal INT,
    suspect_diisolasi INT,
    suspect_discarded INT,
    suspect_meninggal INT,
    tanggal DATE,
)
-- CREATE FACT TABLES
CREATE TABLE Province (
    province_id INT PRIMARY KEY,
    province_name VARCHAR(255)
);

CREATE TABLE District (
    district_id INT PRIMARY KEY,
    province_id INT,
    district_name VARCHAR(255),
    FOREIGN KEY (province_id) REFERENCES Province(province_id)
);

CREATE TABLE Case (
    case_id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) CHECK (status_name IN ('SUSPECT', 'CLOSECONTACT', 'PROBABLE', 'CONFIRMATION')),
    status_detail VARCHAR(255)
);