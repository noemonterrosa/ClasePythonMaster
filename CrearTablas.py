import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import datetime
import os
import io

#Nos aseguramos que estamos en el directorio correcto, en este caso hay que ir a la carpeta Proyecto2
print(os.getcwd())
os.chdir('Proyecto2')
print(os.getcwd())
#Nos conectamos a la Base de Datos
config = configparser.ConfigParser()
config.read('escec.cfg')

rdsIdentifier = 'stocks' #nombre de la instancia
aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')

rdsInstanceIds = []
try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)

import sql_queries
try:
    db_conn = psycopg2.connect(
        database=config.get('RDS', 'DB_NAME'), 
        user=config.get('RDS', 'DB_USER'),
        password=config.get('RDS', 'DB_PASSWORD'), 
        host=RDS_HOST,
        port=config.get('RDS', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(sql_queries.DDL_QUERY)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)

for bucket in s3.buckets.all():
    S3_BUCKET_NAME = bucket.name
    print(bucket.name)
    
S3_BUCKET_NAME = 'bucketnoemaestria'
#extraemos todo lo que está en el bucket
remoteFileList = []
for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
    remoteFileList.append(objt.key)

remoteFileList
#Convertimos archivos csv en dataframes
for remoteFile in remoteFileList:
    try:
        file = s3.Bucket(S3_BUCKET_NAME).Object(remoteFile).get()
        data = file['Body'].read()
        # Obtener el nombre del archivo CSV sin la extensión
        filename = remoteFile.split('.')[0]
        df = pd.read_csv(io.BytesIO(data), delimiter=',')
        # Renombrar el DataFrame utilizando el nombre del archivo CSV
        globals()[filename] = df
        print("El nombre del nuevo archivo DataFrame es:", filename)
    except Exception as ex:
        print("No es un archivo.")
        print(ex)

# Convertir la columna de fecha a un objeto datetime de Pandas y establecer la zona horaria en nulo
sp500_stocks["Date"] = pd.to_datetime(sp500_stocks["Date"], utc=True)
sp500_stocks['year'] = pd.DatetimeIndex(sp500_stocks['Date']).year
sp500_stocks['month'] = pd.DatetimeIndex(sp500_stocks['Date']).month
sp500_stocks['quarter'] = pd.DatetimeIndex(sp500_stocks['Date']).quarter
sp500_stocks['day'] = pd.DatetimeIndex(sp500_stocks['Date']).day
sp500_stocks['week'] = pd.DatetimeIndex(sp500_stocks['Date']).week
sp500_stocks['dayofweek'] = pd.DatetimeIndex(sp500_stocks['Date']).dayofweek
#Eliminamos una buena cantidad de registros para poder trabajar mas comodos
sp500_stocks2 = sp500_stocks[sp500_stocks['year'] > 2020]
sp500_stocks2 = sp500_stocks2.drop("Date", axis=1)
#Creamos la dimension de las fechas
dim_date = sp500_stocks2
dim_date = dim_date.drop("Adj Close", axis=1)
dim_date = dim_date.drop("Close", axis=1)
dim_date = dim_date.drop("High", axis=1)
dim_date = dim_date.drop("Low", axis=1)
dim_date = dim_date.drop("Open", axis=1)
dim_date = dim_date.drop("Volume", axis=1)
dim_date = dim_date.drop("Symbol", axis=1)
dim_date['id_fecha'] = dim_date['year'].astype(str) + '_' + dim_date['month'].astype(str) + '_' + dim_date['day'].astype(str)
dim_date.drop_duplicates(inplace=True)
#Creamos el id de fecha y nos quedamos solo con ese id en la tabla stocks
sp500_stocks2['id'] = sp500_stocks2['year'].astype(str) + '_' + sp500_stocks2['month'].astype(str) + '_' + sp500_stocks2['day'].astype(str) + '_' + sp500_stocks2['Symbol'].astype(str)
sp500_stocks2['id_fecha'] = sp500_stocks2['year'].astype(str) + '_' + sp500_stocks2['month'].astype(str) + '_' + sp500_stocks2['day'].astype(str)
sp500_stocks2 = sp500_stocks2.drop("year", axis=1)
sp500_stocks2 = sp500_stocks2.drop("month", axis=1)
sp500_stocks2 = sp500_stocks2.drop("quarter", axis=1)
sp500_stocks2 = sp500_stocks2.drop("day", axis=1)
sp500_stocks2 = sp500_stocks2.drop("week", axis=1)
sp500_stocks2 = sp500_stocks2.drop("dayofweek", axis=1)
#renombramos las columnas
sp500_stocks2 = sp500_stocks2.rename(columns={'Symbol':'symbol','Adj Close':'adj_close','Close':'close', 'High':'high','Low':'low','Open':'open','Volume':'volume'})
#Trabajamos en sp500index
sp500_index = sp500_index.rename(columns={'S&P500':'sp500'})
# Convertir la columna de fecha a formato datetime
sp500_index['year'] = pd.DatetimeIndex(sp500_index['Date']).year
sp500_index['month'] = pd.DatetimeIndex(sp500_index['Date']).month
sp500_index['day'] = pd.DatetimeIndex(sp500_index['Date']).day
sp500_index['id_fecha'] = sp500_index['year'].astype(str) + '_' + sp500_index['month'].astype(str) + '_' + sp500_index['day'].astype(str)
sp500_index = sp500_index[sp500_index['year'] > 2020]
sp500_index = sp500_index.drop("year", axis=1)
sp500_index = sp500_index.drop("month", axis=1)
sp500_index = sp500_index.drop("Date", axis=1)
sp500_index = sp500_index.drop("day", axis=1)
#Trabajamos en la tabla companies
sp500_companies = sp500_companies.rename(columns={'Exchange':'exchange','Symbol':'symbol','Shortname':'shortname','Longname':'longname', 'Sector':'sector','Industry':'industry','Currentprice':'currentprice',	'Marketcap':'marketcap','Ebitda':'ebitda','Revenuegrowth':'revenuegrowth','City':'city','State':'state','Country':'country', 'Fulltimeemployees':'fulltimeemployees','Longbusinesssummary':'longbusinesssummary','Weight':'weight'})
sp500_companies = sp500_companies.drop("longbusinesssummary", axis=1)
#Para evitar problemas de insercion con las llaves foraneas eliminamos los registros de las compañias
#que no esten en la tabla sp500_companies
valid_symbols = list(sp500_companies['symbol'])
sp500_stocks2 = sp500_stocks2[sp500_stocks2['symbol'].isin(valid_symbols)]
#Algunas estadisticas descriptivas
print(sp500_companies.describe())
print(sp500_index.describe())
print(sp500_stocks2.describe())
#Empezamos a insertar datos
postgres_driver = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""    
try:
          response = sp500_companies.to_sql('dim_companies', postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
except Exception as ex:
          print(ex)
try:
          response = dim_date.to_sql('dim_date', postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
except Exception as ex:
          print(ex)
try:
          response = sp500_index.to_sql('sp500', postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
except Exception as ex:
          print(ex)

try:
          response = sp500_stocks2.to_sql('stock_daily', postgres_driver, index=False, if_exists='append')
          print(f'Se han insertado {response} nuevos registros.' )
except Exception as ex:
          print(ex)
