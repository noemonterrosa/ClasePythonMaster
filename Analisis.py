import pandas as pd
import numpy as np
import boto3
import psycopg2
import configparser
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import datetime
import os
import matplotlib.pyplot as plt

def plot3meses(lista,fig):
    plt.figure(fig)
    # Iteramos sobre cada símbolo y generamos un gráfico de líneas para cada uno
    for symbol in lista:
        df_stocks2 = df_stocks[df_stocks['symbol'] == symbol]
        plt.plot(df_stocks2['date'], df_stocks2['close'], label=symbol)

    # Configuramos la leyenda y los títulos del gráfico
    plt.legend()
    plt.title('Valores de cierre diarios para símbolos seleccionados')
    plt.xlabel('Fecha')
    plt.xticks(rotation=90)
    plt.ylabel('Valor de cierre')

    # Mostramos el gráfico
    plt.show()

def PredRL(df,fig,symbol):
    plt.figure(fig)
    df = df.sort_values('date')
    train, test = train_test_split(df, test_size=0.3, shuffle=False)
    model = LinearRegression()
    X_train = train['day'].values.reshape(-1, 1)
    y_train = train['close'].values
    X_test = test['day'].values.reshape(-1, 1)
    model.fit(X_train, y_train)
    # Hacer la predicción para los siguientes 5 días
    y_pred = model.predict(X_test)
    X_test = X_test.reshape(-1)
    pred_df = pd.DataFrame({'day': X_test, 'close_pred': y_pred})
    # Graficar los valores de cierre y las predicciones
    plt.plot(df['day'], df['close'],  label='close')
    plt.plot(pred_df['day'], pred_df['close_pred'], label='pred')
    plt.title(f'Prediccion de 3 dias para {symbol}, mes Enero 2023')
    plt.xlabel('Dia')
    plt.xticks(rotation=90)
    plt.ylabel('Valor de cierre')
    plt.legend()
    plt.show()

#Nos aseguramos que estamos en el directorio correcto, en este caso hay que ir a la carpeta Proyecto2
print(os.getcwd())
os.chdir('Proyecto2')
print(os.getcwd())
#Nos conectamos a la Base de Datos
config = configparser.ConfigParser()
config.read('escec.cfg')
RDS_HOST = 'stocks.csg44mdgnhhv.us-east-1.rds.amazonaws.com'
postgres_driver = f"""postgresql://{config.get('RDS', 'DB_USER')}:{config.get('RDS', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS', 'DB_PORT')}/{config.get('RDS', 'DB_NAME')}"""
#Extraemos la primera dimension, la de compañias y dividimos todo en 3 clusters
sql_query = 'SELECT * FROM dim_companies;'
df_companies = pd.read_sql(sql_query, postgres_driver)
df_clusters_companies = pd.qcut(df_companies['currentprice'], q=3, labels=False)
df_cluster1 = df_companies[df_clusters_companies == 0]
df_cluster2 = df_companies[df_clusters_companies == 1]
df_cluster3 = df_companies[df_clusters_companies == 2]
#Aqui podemos mostrar cuales son los limites de cada cluster
maxC1 = df_cluster1.describe()['currentprice'][[ 'max']][0]
maxC2 = df_cluster2.describe()['currentprice'][[ 'max']][0]
maxC3 = df_cluster3.describe()['currentprice'][[ 'max']][0]
print(f'Max C1 es:{maxC1}, Max C2 es: {maxC2}, Max C3 es: {maxC3}')
#Las primeras 3 preguntas que nos planteamos son: Cuales son las  5 acciones que han tenido mejor
#desempeño en el ultimo mes (13 Dic 22 al 13 ENE 23) en el cluster 1, 2 y 3?
#Primero sacamos el valor en 13 dic 22 y lo unimos a las tablas de los cluster donde tenemos el valor actual
sql_query = "SELECT * FROM stock_daily where id_fecha ='2022_12_13' ;"
df_stocks_20231213 = pd.read_sql(sql_query, postgres_driver)

df_cluster1 = pd.merge(df_cluster1, df_stocks_20231213[['symbol', 'close']], on='symbol', how='left')
df_cluster2 = pd.merge(df_cluster2, df_stocks_20231213[['symbol', 'close']], on='symbol', how='left')
df_cluster3 = pd.merge(df_cluster3, df_stocks_20231213[['symbol', 'close']], on='symbol', how='left')
#Sacamos la diferencia
df_cluster1["diff"] = df_cluster1["currentprice"] - df_cluster1["close"]
df_cluster2["diff"] = df_cluster2["currentprice"] - df_cluster2["close"]
df_cluster3["diff"] = df_cluster3["currentprice"] - df_cluster3["close"]
#Obtenemos las mejores 5 acciones en cada cluster
top_5_c1 = df_cluster1.sort_values(by='diff', ascending=False).head(5)
top_5_c2 = df_cluster2.sort_values(by='diff', ascending=False).head(5)
top_5_c3 = df_cluster3.sort_values(by='diff', ascending=False).head(5)
#Las siguientes preguntas del analisis es como se han desempeñado estas acciones en los ultimos 3 meses, para cada cluster?
#Al archivo principal de las acciones le unimos las columnas del tiempo
sql_query = "SELECT * FROM stock_daily ;"
df_stocks = pd.read_sql(sql_query, postgres_driver)
sql_query = "SELECT * FROM dim_date ;"
df_dim_date = pd.read_sql(sql_query, postgres_driver)
df_stocks = pd.merge(df_stocks, df_dim_date[['id_fecha','year','month','day']], on='id_fecha', how='left')

df_stocks['date'] = pd.to_datetime(df_stocks[['year', 'month', 'day']])

# Filtrar las filas que están dentro del rango de fechas
start_date = pd.to_datetime('2022-10-01')
end_date = pd.to_datetime('2023-01-13')
df_stocks = df_stocks.loc[(df_stocks['date'] >= start_date) & (df_stocks['date'] <= end_date)]
#Sacamos una lista para cada cluster
lista_top5_c1 = top_5_c1['symbol'].tolist()
lista_top5_c2 = top_5_c2['symbol'].tolist()
lista_top5_c3 = top_5_c3['symbol'].tolist()
#Hacemos las graficas para cada top5 de cad cluster
plot3meses(lista_top5_c1,1)
plot3meses(lista_top5_c2,2)
plot3meses(lista_top5_c3,3)

#Siguiente set de preguntas
#Queremos sacar una prediccion basica para la mejor accion de cada cluster, desde el 14 de enero al 1 de abril 2023
#Utilizaremos un modelo de regresion lineal
# Filtrar las filas que están dentro del rango de fechas
start_date = pd.to_datetime('2023-01-01')
end_date = pd.to_datetime('2023-01-30')
df_stocks_P = df_stocks.loc[(df_stocks['date'] >= start_date) & (df_stocks['date'] <= end_date)]
df_stocks_P.head()

#Regresion Linear con scikit learn
symbolc1 = lista_top5_c1[0]
symbolc2 = lista_top5_c2[0]
symbolc3 = lista_top5_c3[0]

df_stockstop5_c1_1 = df_stocks_P[df_stocks_P['symbol'] == symbolc1]
df_stockstop5_c2_1 = df_stocks_P[df_stocks_P['symbol'] == symbolc2]
df_stockstop5_c3_1 = df_stocks_P[df_stocks_P['symbol'] == symbolc3]

PredRL(df_stockstop5_c1_1,1,symbolc1)
PredRL(df_stockstop5_c2_1,1,symbolc2)
PredRL(df_stockstop5_c3_1,1,symbolc3)
