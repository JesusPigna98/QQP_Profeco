from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,DateType
import os
from datetime import datetime
jdbc_driver_path = '/usr/local/lib/postgresql-42.7.4.jar'
spark = SparkSession.builder \
        .appName('QQP') \
        .config("spark.jars",jdbc_driver_path) \
        .getOrCreate()




def create_dataframe(files_dir):
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
    
    print("Creando dataframes..")
    for file in os.listdir(files_dir):
        df = spark.read.option('header',False).csv(f'{files_dir}/{file}',schema=schema)
        df_list.append(df)
    
    return df_list


def merge_dataframes(df_list):
    print("Haciendo merge de dataframes..")
    if len(df_list) > 1:
        merged_df = df_list[0]
        for df in df_list[1:]:
            merged_df = merged_df.unionAll(df)
        
        return merged_df

    else:
        return df_list[0]





