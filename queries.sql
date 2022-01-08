---PRIMERO CREO LAS COLUMNAS FALTANTES DE LA TABLA MUSEOS
ALTER TABLE museos
ADD COLUMN IF NOT EXISTS categoria text;

UPDATE museos
SET categoria = 'Museos';

ALTER TABLE museos
ADD COLUMN IF NOT EXISTS id_departamento bigint;

DROP TABLE IF EXISTS centros_culturales;

---CREO LA TABLA CENTROS CULTURALES LA CUAL SE NUTRE DE MUSEOS, CINES Y BIBLIOTECAS.
CREATE TABLE centros_culturales AS
(SELECT 
m.categoria,
m.nombre,
m.localidad_id AS cod_localidad,
m.localidad,
m.provincia_id AS id_provincia,
m.provincia,
m.id_departamento,
m.direccion AS domicilio,
m.codigo_postal,
CAST(m.telefono AS text) AS telefono,
m.mail,
m.web,
m.fecha
FROM museos m)
UNION 
(SELECT
b."categoria" AS categoria,
b."nombre",
b."cod_loc" AS cod_localidad,
b."localidad",
b."idprovincia" AS id_provincia,
b."provincia",
b."iddepartamento" AS id_departamento,
b."domicilio",
b."cp" AS codigo_postal,
b."telefono" AS telefono,
b."mail",
CAST(b."web" AS text) AS web,
b."fecha"
FROM bibliotecas b)
UNION
(SELECT
c."categoria" AS categoria,
c."nombre",
c."cod_loc" AS cod_localidad,
c."localidad",
c."idprovincia" AS id_provincia,
c."provincia",
c."iddepartamento" AS id_departamento,
c."direccion" AS domicilio,
CAST(c."cp" AS text) AS codigo_postal,
c."telefono" AS telefono,
c."mail",
c."web",
c."fecha"
FROM cines c);

--Reemplazo los valores "sin datos" por NULL
UPDATE centros_culturales
SET mail = NULL
WHERE mail = 's/d';

UPDATE centros_culturales
SET telefono = NULL
WHERE telefono = 's/d';

UPDATE centros_culturales
SET codigo_postal = NULL
WHERE codigo_postal = 's/d';

UPDATE centros_culturales
SET web = NULL
WHERE web = 's/d';

DROP TABLE IF EXISTS tabla_cines;

---CREO LA TABLA TABLA_CINES LA CUAL SE NUTRE DE CINES
CREATE TABLE tabla_cines AS
SELECT 
c."provincia",
COUNT(*) AS cines,
SUM(c."pantallas") AS cantidad_pantallas,
SUM(c."butacas") AS cantidad_butacas,
COUNT(c."espacio_incaa") AS cant_esp_incaa,
c."fecha"
FROM cines c
GROUP BY provincia, fecha;

DROP TABLE IF EXISTS registro_1;

---CREO TABLAS INDIVIDUALES DE CONSULTAS Y TABLA CONJUNTA DE REGISTROS
---cantidad de registros totales por categoria
CREATE TABLE registro_1 AS
SELECT 
categoria,
COUNT(*) AS cantidad,
fecha
FROM centros_culturales
GROUP BY categoria, fecha;

DROP TABLE IF EXISTS registro_2;

---cantidad de registros por provincia y categoria
CREATE TABLE registro_2 AS
SELECT 
categoria,
provincia,
COUNT(*) AS cantidad,
fecha
FROM centros_culturales
GROUP BY categoria, provincia, fecha
ORDER BY provincia ASC;

DROP TABLE IF EXISTS registro_3;

---cantidad de registros totales por fuente
CREATE TABLE registro_3 AS
(SELECT
c."fuente",
c."categoria" AS categoria,
COUNT(*) AS cantidad,
c."fecha"
FROM cines c
GROUP BY fuente, categoria, fecha)
UNION
(SELECT
m."fuente",
m."categoria",
COUNT(*) AS cantidad,
m."fecha"
FROM museos m
GROUP BY fuente, categoria, fecha)
UNION
(SELECT
b."fuente",
b."categoria" AS categoria,
COUNT(*) AS cantidad,
b."fecha"
FROM bibliotecas b
GROUP BY fuente, categoria, fecha);


DROP TABLE IF EXISTS registros;

CREATE TABLE registros AS
(SELECT 
categoria,
's/d' AS provincia,
fuente,
cantidad,
fecha
FROM registro_3)
UNION
(SELECT
categoria,
provincia,
's/d' AS fuente,
cantidad,
fecha
FROM registro_2)
UNION
(SELECT
categoria,
'todas' AS provincia,
's/d' AS fuente,
cantidad,
fecha
FROM registro_1);