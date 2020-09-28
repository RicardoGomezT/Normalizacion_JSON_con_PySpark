from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import json
from pyspark.sql import HiveContext
from pyspark.sql import SQLContext
from sys import argv, path as pythonpath
from os import getcwd, path as ospath
pythonpath.insert(0, getcwd())
###############################################
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import lower, col
from pyspark.sql import HiveContext
from pyspark import sql
#from utils import *
import pytz
import datetime
import os
import json
import argparse
from pyspark.sql.functions import lit
from pyspark.sql.functions import unix_timestamp
from pyspark.sql import functions as F
import pandas as pd
from pandas.io.json import json_normalize
from pyspark.sql.functions import explode
from pyspark.sql.functions import *
from pyspark.sql.types import *

conf =SparkConf().setAppName("DF_Spark").setMaster("local[3]")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)

print ("-----------------CARGUE DE META DATA----------")
TopicoIN = 'REST'

metadata_path="./kafka/metadata_JSON.csv"
metadata = sc.textFile( metadata_path ).map( lambda x: x.split(";") ).collect()
esquema_1= StructType([StructField("json_path", StringType(), True), StructField("nombre_tabla",StringType(), True), StructField("tipo_dato", StringType(), True), StructField("estructura", StringType(), True)])
DF_parametrizacion = sqlContext.createDataFrame(metadata, esquema_1)

metadata_path="./kafka/metadata_maestra.csv"
metadata_maestra = sc.textFile( metadata_path ).map( lambda x: x.split(";") ).collect()
esquema_2= StructType([StructField("Topico", StringType(), True), StructField("estructura",StringType(), True), StructField("PathUnico", StringType(), True), StructField("CamposConArrays", StringType(), True), StructField("tabla_1", StringType(), True), StructField("tabla_2", StringType(), True)])
DF_Maestra = sqlContext.createDataFrame(metadata_maestra, esquema_2)

print ("DF_parametrizacion: ")
DF_parametrizacion.show()

print ("DF_Maestra: ")
DF_Maestra.show()

print("DF_Maestra_Filtrada")
DF_Maestra_temp = DF_Maestra.filter(DF_Maestra['Topico'] == TopicoIN)
DF_Maestra_temp.show()
Estructura = DF_Maestra_temp.collect() [0] [1]
Path_Unico = DF_Maestra_temp.collect() [0] [2]
Campos_Array = DF_Maestra_temp.collect() [0] [3]
tabla_1 = DF_Maestra_temp.collect() [0] [4]
tabla_2 = DF_Maestra_temp.collect() [0] [5]

print("DF_parametrizacion_Filtrada: ")
DF_parametrizacion_temp_1 = DF_parametrizacion.filter ((DF_parametrizacion['tipo_dato'] != "Array") & (DF_parametrizacion['estructura'] == Estructura))
DF_parametrizacion_temp_2 = DF_parametrizacion_temp_1.collect()
DF_parametrizacion_temp_1.show()

print("DF_parametrizacion_Array_Filtrada: ")
DF_parametrizacion_Array_temp_1 = DF_parametrizacion.filter (DF_parametrizacion['tipo_dato'] == "Array")
DF_parametrizacion_Array_temp_2 = DF_parametrizacion_Array_temp_1.collect()
DF_parametrizacion_Array_temp_1.show()

print ("Topico de entrada: {}".format(TopicoIN))
print ("Estructura       : {}".format(Estructura))
print ("Path_Unico       : {}".format(Path_Unico))
print ("Campos_Array     : {}".format(Campos_Array))
print ("tabla_1          : {}".format(tabla_1))
print ("tabla_2          : {}".format(tabla_2))

#Dataframe_Data_Cruda
#Columnas_data_cruda = DF_parametrizacion_temp_2.columns['json_path']
#Columnas_data_cruda.show()
#DF_Data_Cruda = sqlContext.createDataFrame(metadata_maestra, esquema_2)

print ("-----------------LECTURA JSON ENTRADA-----------------------")

df= sqlContext.read.json("kafka/Evaluador_consola.json") #HDFS usuario s-aprov_prot   Evaluador_consola
df.printSchema()

print ("------PROCESAMIENTO DATA_ CRUDA --------------")

dato_1 = df.select(DF_parametrizacion_temp_2 [0] [0]).collect()[0][0] #obtecto el valor del json de entrada para ese path
dato_2 = 20191210
columna_1 = DF_parametrizacion_temp_2 [0] [1]
columna_2 = DF_parametrizacion_temp_2 [1] [1]
DF_Data_Cruda= sqlContext.createDataFrame([(dato_1, dato_2)], [columna_1, "tiempo_en"]) #DF_Data_Cruda.show()

for x in range (1, DF_parametrizacion_temp_1.count()):
	try :
		path = DF_parametrizacion_temp_2 [x] [0] #obtengo el path del DF parametrizacion
		dato_prueba = df.select(path).collect()[0][0] #obtecto el valor del json de entrada para ese path
		dato_tipo = DF_parametrizacion_temp_2 [x] [2]
		DF_Data_Cruda = DF_Data_Cruda.withColumn(DF_parametrizacion_temp_2 [x] [1], lit(dato_prueba).cast(dato_tipo))
	except:
		#print ("Path no encontrado: {}".format(path))
		DF_Data_Cruda = DF_Data_Cruda.withColumn(DF_parametrizacion_temp_2 [x] [1], lit(None))

DF_Data_Cruda = DF_Data_Cruda.drop('tiempo_en')
DF_Data_Cruda= DF_Data_Cruda.withColumn("tiempo_en", lit(dato_2).cast("int"))
print ("DF_Data_Cruda: ")
DF_Data_Cruda.show(n=5)

print ("------PROCESAMIENTO DATA_ARRAYS --------------")


esquema_Arrays= StructType([StructField("con_appcon_transaction_id",StringType(), True),StructField("numero_arreglo",IntegerType(), True), StructField("path_origen",StringType(), True), StructField("name",StringType(), True), StructField("value", StringType(), True), StructField("formato", StringType(), True), StructField("references", StringType(), True), StructField("id_service", StringType(), True), StructField("request_service", StringType(), True), StructField("response_service", StringType(), True), StructField("tiempo_en",IntegerType(), True)])
Lista_Campos_Array = Campos_Array.split("&")
Dato_unico = df.select(Path_Unico).collect()[0][0]
cont = 0
ll= []
for x in range (0, DF_parametrizacion_Array_temp_1.count()):
    tup = ()
    Lista_Data= DF_parametrizacion_Array_temp_2 [x][1]
    for y in range (0, len (Lista_Campos_Array)):
        path = DF_parametrizacion_Array_temp_2 [x] [0] + "." + Lista_Campos_Array [y]
        try:
            dato_prueba = df.select(path).collect()[0][0][0]
            tup = tup + (dato_prueba,)
        except Exception as e:
            #print ("Path no encontrado: {}".format(path))
            tup = tup + (None,)
            #print(e)
    tupNull= (None, None, None, None, None, None, None)
    if tup!=tupNull:
        tiempo_en= 20191212
        cont = cont + 1
        path_origen = DF_parametrizacion_Array_temp_2 [x][1]
        tup = (Dato_unico,) + (cont,) + (path_origen,) + tup + (tiempo_en,)
        ll.append(tup)


DF_Data_Array= sqlContext.createDataFrame(ll, esquema_Arrays)
print ("DF_Data_Array: ")
DF_Data_Array.show()

print ("------------------FIN-------------------------")

