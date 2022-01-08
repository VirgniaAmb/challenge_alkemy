import pandas as pd
import datetime as dt
import requests
import io
from sqlalchemy import create_engine, text
import os
from decouple import config
import logging

# URL centros culturales Argentina
URL_MUSEOS = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv'
URL_CINES = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv'
URL_BIBLIOTECAS = 'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv'

#
DB_USER = config('DB_USER')
DB_PASSWORD = config('DB_PASSWORD')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')

SQL_SCRIPT = 'queries.sql'

# basic logging config
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG, datefmt='%I:%M:%S')

dict_cc = {
  'museos': {
      'url': URL_MUSEOS,
      'filename': '',
      'content': '',
      'df': ''
   },
  'cines': {
      'url': URL_CINES,
      'filename': '',
      'content': '',
      'df': ''
   },
  'bibliotecas': {
      'url': URL_BIBLIOTECAS,
      'filename': '',
      'content': '',
      'df': ''
   }
}


def download(c, url):
    '''Descarga dado una url los archivos de la fuente utilizando la libreria Request
    Toma una variable representando un centro cultural (c) y la url correspondiente
    Devuelve el contenido de la request y el nombre del archivo calculado.'''
    try:
        req = requests.get(url)
        logger.info('Status code {}'.format(req.status_code))
        content = req.content
        filename = '{name}\{year}-{month}\{name}-{date}.csv'.format(date=dt.date.today(),
                                                                    year=dt.date.today().year,
                                                                    month=dt.date.today().month,
                                                                    name=c)
        logger.info('Source Filename: {}'.format(filename))

    except ValueError as e:
        logger.error(e)

    return content, filename


def write(filename, content):
    '''Realiza la escritura del CSV. Toma el nombre del archivo y el contenido
    del mismo y escribe el contenido dentro de un archivo csv.'''
    try:
        csv = open(filename, 'wb')
        csv.write(content)
        csv.close()
        logger.info('CSV escrito correctamente')
    except ValueError as e:
        logger.error(e)


def connection_db():
    '''Realizamos la conexion a la base de datos con SQLalchemy
    Devuelve la variable engine ya conectado segun los parametros'''
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/centros_culturales".format(),
                               echo=False, client_encoding='utf8')
        logger.info('Conexion exitosa a la base de datos')
        return engine

    except ValueError as e:
        logger.error(e)


def load(filename, engine, c):
    '''Toma el nombre, la conexion a la base (engine) y el tipo de cada centro
    cultural. Crea un dataframe y escribe una tabla a partir del engine
    ingestando los datos del archivo en dicha tabla.'''
    try:
        df = pd.read_csv(filename, encoding="unicode_escape")
        logger.info('read_csv exitoso')
        df['fecha'] = dt.date.today()
        df.columns = df.columns.str.lower()
        df = df.rename(columns={'categorã­a': 'categoria', 'direcciã³n': 'direccion', 'telã©fono': 'telefono'})
        df.to_sql(c, con=engine, if_exists="replace")
        logger.info('Datos ingestados en la base de datos')
        logger.info('Cantidad de registros en el archivo: {}'.format(len(df.index)))
    except ValueError as e:
        logger.error(e)


def transformation(engine):
    '''Toma la conexion a la base de datos engine y a partir del sql.script
    definido y ejecuta sus queries definidos sobre la base'''
    try:
        sql_file = open(SQL_SCRIPT)
        sql_as_string = sql_file.read()
        with engine.connect() as conn:
            rs = conn.execute(text(sql_as_string))
    except Exception as e:
        logger.error(e)


def main():

    logger.info('Comienza la extraccion')

    engine = connection_db()

    for c in dict_cc.keys():
        logger.info('centro cultural en proceso {}'.format(c))
        content, filename = download(c, dict_cc[c]['url'])
        dict_cc[c]['content'] = content
        dict_cc[c]['filename'] = filename

        write(dict_cc[c]['filename'], dict_cc[c]['content'])

        load(dict_cc[c]['filename'], engine, c)

    transformation(engine)

if __name__ == "__main__":
    logger.info('ETL Process Initialized')
    main()