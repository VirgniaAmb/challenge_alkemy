# Challenge Alkemy by Virginia Ambrosino

## Configuración

Configuramos un archivo .env con la siguiente informacion

DB_USER="user"
DB_PASSWORD="password"
DB_HOST="host"
DB_PORT="port"

## Instalación

Pasos para instalar los paquetes requeridos como un nuevo entorno de Conda:

1. Creamos el entorno local:

        conda create --name <name> python=3.8
        conda activate <name> 
    
2. Instalamos librerías:
    
    conda install pandas
    conda install sqlalchemy
    pip install python-decouple
    pip install psycopg2-binary

## Ejecución

En esta oportunidad cree una base de datos en PostgreSQL en forma manual, para poder conectarme desde Python.

El script de python contiene los pasos que conforman un ELT (primero hago la carga y luego la transformacion).

Para correrlo es necesario, una vez configurado el archivo .env y activado el ambiente con las librerias correspondientes,
correr >> python challenge_alkemy.py
    
## Archivos Fuente

Contiene 3 archivos:
    challenge_alkemy.py (script de python)
    queries.sql (script de SQL)
    README.md

## Procesamiento de datos

Transformamos los archivos fuente con la base de datos PostgreSQL. Se crearon las columnas necesarias para la union de tablas,
se actualizaron los registros sin datos por NULL y se crearon las nuevas tablas.

## Algunos links útiles

* [Como armar un ETL] https://towardsdatascience.com/what-to-log-from-python-etl-pipelines-9e0cfe29950e
* [Como usar Python-decouple] https://simpleisbetterthancomplex.com/2015/11/26/package-of-the-week-python-decouple.html
* [Request] https://www.kite.com/python/answers/how-to-download-a-csv-file-from-a-url-in-python
* [pandas.DataFrame.to_sql] https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html






