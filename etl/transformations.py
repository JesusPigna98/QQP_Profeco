from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType
import os
from datetime import datetime
spark = SparkSession.builder.appName('QQP').getOrCreate()

#df1 = spark.read.csv('/files/')

def createDataframe():
    schema = StructType([
        StructField('producto',StringType()),
        StructField('presentacion',StringType()),
        StructField('marca',StringType()),
        StructField('categoria',StringType()),
        StructField('catalogo',StringType()),
        StructField('precio',FloatType()),
        StructField('fecharegistro',DateType()),
        StructField('cadenacomercial',StringType()),
        StructField('giro',StringType()),
        StructField('nombrecomercial',StringType()),
        StructField('direccion',StringType()),
        StructField('estado',StringType()),
        StructField('municipio',StringType()),
        StructField('latitud',FloatType()),
        StructField('longitud',FloatType())
        ])

    df_list = []
    year = datetime.now().year
    files_dir = os.getcwd() + f'/files/{year}'

    for file in os.listdir(files_dir):
        df = spark.read.option('header',False).csv(f'{files_dir}/{file}',schema=schema)
        df_list.append(df)
    
    return df_list


def mergeDataframes(df_list):
    if len(df_list) > 1:
        df1 = df_list[0]
        for df in df_list[1:]:
            merged_df = df1.unionAll(df)
        
        return merged_df

    else:
        return df_list[0]




