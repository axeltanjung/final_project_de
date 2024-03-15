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