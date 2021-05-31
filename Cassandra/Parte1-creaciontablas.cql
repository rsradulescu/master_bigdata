--------------------------------------------
-- Radulescu Rocio
-- Actividad 2 de portfolio
--------------------------------------------

-- -- Ejercicio 1 - creacion de tablas obtenidas en el el portfolio 1 -- --

CREATE KEYSPACE rocioradulescu
WITH durable_writes = true
AND replication = {
	'class' : 'SimpleStrategy',
	'replication_factor' : 1
};

-- Ejercicio 1: jefes_provinciales_provincia
CREATE TABLE rocioradulescu.jefes_provinciales_provincia (
	provincia_nombre text,
	provincia_codpro int,
    provincia_jefes_provinciales set<text>,
	PRIMARY KEY(provincia_nombre, provincia_codpro)
) WITH CLUSTERING ORDER BY (provincia_codpro ASC);

-- Ejercicio 2: jefes_provinciales_zona
CREATE TABLE rocioradulescu.jefes_provinciales_zona (
	zona_nombre text,
	zona_zoncod int,
    provincia_jefes_provinciales set<text>,
	PRIMARY KEY (zona_nombre, zona_zoncod)
) WITH CLUSTERING ORDER BY (zona_zoncod ASC);

-- Ejercicio 3: distribucion_red_estacion
CREATE TABLE rocioradulescu.distribucion_red_estacion (
	estacion_codest int,
	num_distribucion_de_red_coddis counter,
	PRIMARY KEY (estacion_codest)
);
-- Ejercicio 4: subestacion_longitud_lineas
CREATE TABLE rocioradulescu.subestacion_longitud_lineas (
	subestacion_capacidad float,
	subestacion_codsub int,
    linea_longitud float,
	PRIMARY KEY (subestacion_capacidad, subestacion_codsub)
) WITH CLUSTERING ORDER BY (subestacion_codsub ASC);

-- Ejercicio 5: provincia_jefes_provinciales
CREATE TABLE rocioradulescu.provincia_jefes_provinciales (
	provincia_jefe_provincial text,
	provincia_provcod int,
    provincia_nombre text,
	PRIMARY KEY (provincia_jefe_provincial, provincia_provcod)
) WITH CLUSTERING ORDER BY (provincia_provcod ASC);

-- Ejercicio 6: zonas_subestaciones_fecha
CREATE TABLE rocioradulescu.zonas_subestaciones_fecha (
	distribuye_fecha date,
	subestacion_codsub int,
	zona_zoncod int,
	zona_nombre text,
	subestacion_capacidad float,
	PRIMARY KEY (distribuye_fecha, subestacion_codsub, zona_zoncod)
) WITH CLUSTERING ORDER BY (subestacion_codsub ASC, zona_zoncod ASC);

-- Ejercicio 7: subestaciones_estacion_linea
CREATE TABLE rocioradulescu.subestaciones_estacion_linea (
	estacion_nombre text,
	subestacion_codsub int,
	estacion_codest int,
	linea_longitud float,
	PRIMARY KEY (estacion_nombre, subestacion_codsub, estacion_codest)
) WITH CLUSTERING ORDER BY (subestacion_codsub ASC, estacion_codest ASC);

-- Ejercicio 8: provincias_linea
CREATE TABLE rocioradulescu.provincias_linea (
	linea_longitud float,
	subestacion_codsub int,
	zona_zoncod int,
	provincia_nombre text,
    provincia_procod int,
	PRIMARY KEY (linea_longitud, subestacion_codsub, zona_zoncod)
) WITH CLUSTERING ORDER BY (subestacion_codsub ASC, zona_zoncod ASC);

-- Ejercicio 9: capacidad_subestaciones_zona
CREATE TABLE rocioradulescu.capacidad_subestaciones_zona (
	zona_zoncod int,
	num_subestacion_capacidad counter,
	PRIMARY KEY (zona_zoncod)
);

-- Ejercicio 10: productores_provincia_lineas_estaciones
CREATE TABLE rocioradulescu.productores_provincia_lineas_estaciones (
	provincia_nombre text,
	productor_nombre text,
    productor_codpro int,
    estacion_codest int,
    subestacion_codsub int,
    zona_zoncod int,
    linea_longitud float,
    linea_codlin int,
    estacion_nombre text,
	PRIMARY KEY ((provincia_nombre, productor_nombre), productor_codpro, estacion_codest, subestacion_codsub, zona_zoncod)
) WITH CLUSTERING ORDER BY (productor_codpro ASC, estacion_codest ASC, subestacion_codsub ASC, zona_zoncod ASC);

-- Ejercicio 11: productores_estacion_distribucion
CREATE TABLE rocioradulescu.productores_estacion_distribucion (
	distribuciondered_longitudmaxima float,
    productor_codpro int,
    estacion_codest int,
    distribuciondered_coddis int,
    productor_pais text,
    productor_nombre text,
	PRIMARY KEY (distribuciondered_longitudmaxima, productor_codpro, estacion_codest, distribuciondered_coddis)
) WITH CLUSTERING ORDER BY (productor_codpro ASC, estacion_codest ASC, distribuciondered_coddis ASC);

-- Ejercicio 12: productor_origen_energia
CREATE TABLE rocioradulescu.productor_origen_energia (
	productor_origen_energia text,
    productor_codpro int,
    productor_nombre text,
	productor_mediaproduccion float,
    PRIMARY KEY (productor_origen_energia, productor_codpro)
) WITH CLUSTERING ORDER BY (productor_codpro ASC);

-- Creacion de tablas soporte para actividad 2 -- --

CREATE TABLE rocioradulescu.productor (
	codpro int,
    origen_energia text,
    nombre text,
    pais text,
	maximoproduccion float,
    mediaproduccion float,
    PRIMARY KEY (codpro)
);

CREATE TABLE rocioradulescu.provincia (
	codpro int,
    nombre text,
    jefes_provinciales set<text>,
	PRIMARY KEY (codpro)
);

CREATE TABLE rocioradulescu.distribuye (
	codsub int,
    zoncod int,
    cantidad float,
    fecha date,
	PRIMARY KEY (codsub, zoncod)
) WITH CLUSTERING ORDER BY (zoncod ASC);

CREATE TABLE rocioradulescu.suple_subestacion (
	codsub int,
    codlin int,
    capacidad float,
	PRIMARY KEY (codsub)
);