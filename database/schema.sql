-- Definimos la base de datos --
USE visitas_db;

-- Creamos la tabla "visitantes" (Esta estaba perfecta) --
CREATE TABLE visitantes ( 
    email VARCHAR(255) NOT NULL,
    fechaPrimeraVisita DATE,
    fechaUltimaVisita DATE, 
    visitasTotales INT DEFAULT 0,
    visitasAnioActual INT DEFAULT 0,
    visitasMesActual INT DEFAULT 0,
    
    PRIMARY KEY (email)
);


-- Creamos tabla "estadisticas" --
CREATE TABLE estadisticas (
    idEstadistica INT AUTO_INCREMENT NOT NULL,
    email VARCHAR(255), 
    jyv VARCHAR(255),
    badMail VARCHAR(255),
    baja VARCHAR(255),
    fechaEnvio DATETIME, -- DATETIME para mayor trazabilidad
    fechaOpen DATETIME,  
    opens INT DEFAULT 0,
    opensVirales INT DEFAULT 0,
    fechaClick DATETIME,  
    clicks INT DEFAULT 0,
    clicksVirales INT DEFAULT 0,
    links TEXT, 	-- Los links pueden ser muy largos
    ips VARCHAR(255),
    navegadores VARCHAR(255),
    plataformas VARCHAR(255),
    
    PRIMARY KEY (idEstadistica),
    FOREIGN KEY (email) REFERENCES visitantes(email)
);


-- Creamos tabla "errores" --
CREATE TABLE errores ( 
    idError INT AUTO_INCREMENT NOT NULL,
    nombreArchivo VARCHAR(255),
    email VARCHAR(255),
    tipoError VARCHAR(255),
    fechaError DATETIME DEFAULT CURRENT_TIMESTAMP, -- DATETIME automático para mayor trazabilidad
    
    PRIMARY KEY (idError)
);
	





	
	
	
	
	
	
	
	
	
	
	
	
	


