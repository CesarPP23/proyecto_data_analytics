-- 1. Crear la base de datos
CREATE DATABASE NetflixDB;
GO

-- 2. Usar la base de datos recién creada
USE NetflixDB;
GO

-- 3. Crear la tabla principal de títulos
CREATE TABLE titles (
    show_id VARCHAR(20) PRIMARY KEY,
    type VARCHAR(20),
    title NVARCHAR(255),
    country NVARCHAR(255),
    date_added DATE,
    release_year INT,
    rating VARCHAR(20),
    duration_int INT,
    duration_type VARCHAR(20),
    description NVARCHAR(MAX)
);

-- 4. Crear la tabla de directores
CREATE TABLE directors (
    director_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) UNIQUE
);

-- 5. Crear la tabla de actores/actrices
CREATE TABLE casts (
    cast_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) UNIQUE
);

-- 6. Crear la tabla de géneros
CREATE TABLE genres (
    genre_id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) UNIQUE
);

-- 7. Crear la tabla de relación títulos-directores
CREATE TABLE title_director (
    show_id VARCHAR(20) NOT NULL,
    director_id INT NOT NULL,
    PRIMARY KEY (show_id, director_id),
    FOREIGN KEY (show_id) REFERENCES titles(show_id),
    FOREIGN KEY (director_id) REFERENCES directors(director_id)
);

-- 8. Crear la tabla de relación títulos-cast
CREATE TABLE title_cast (
    show_id VARCHAR(20) NOT NULL,
    cast_id INT NOT NULL,
    PRIMARY KEY (show_id, cast_id),
    FOREIGN KEY (show_id) REFERENCES titles(show_id),
    FOREIGN KEY (cast_id) REFERENCES casts(cast_id)
);

-- 9. Crear la tabla de relación títulos-géneros
CREATE TABLE title_genre (
    show_id VARCHAR(20) NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (show_id, genre_id),
    FOREIGN KEY (show_id) REFERENCES titles(show_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);