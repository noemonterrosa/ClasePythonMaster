import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
import os

print(os.getcwd())
os.chdir('Proyecto2')
print(os.getcwd())


config = configparser.ConfigParser()
result = config.read('escec.cfg')
# Verifica si el archivo se leyó correctamente
if len(result) == 0:
    print('No se pudo leer el archivo de configuración.')
else:
    print('El archivo de configuración se leyó correctamente.')

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

def leer_archivos():
    #extraemos todo lo que está en el bucket
    remoteFileList = []
    for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
        remoteFileList.append(objt.key)

    remoteFileList
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
            return filename
        except Exception as ex:
            print("No es un archivo.")
            print(ex)