# utils.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import unicodedata
from config import DB_CONFIG


# Spark Session (con JDBC driver para PostgreSQL)
def get_spark_session():
    return SparkSession.builder \
        .appName("DWH_MP3") \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .config("spark.driver.extraClassPath", "postgresql-42.7.5.jar") \
        .getOrCreate()


# Función para cargar una tabla desde PostgreSQL
def cargar_tabla(nombre_tabla, spark):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}") \
        .option("dbtable", nombre_tabla) \
        .option("user", DB_CONFIG['user']) \
        .option("password", DB_CONFIG['password']) \
        .option("driver", DB_CONFIG['driver']) \
        .load()


# Función para guardar un DataFrame como tabla en PostgreSQL
def guardar_tabla(df, nombre_tabla):
    df.write \
      .format("jdbc") \
      .option("url", f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}") \
      .option("dbtable", nombre_tabla) \
      .option("user", DB_CONFIG['user']) \
      .option("password", DB_CONFIG['password']) \
      .option("driver", DB_CONFIG['driver']) \
      .mode("overwrite") \
      .save()


# Función de limpieza de texto
def limpiar_texto(texto):
    if texto:
        texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
        texto = texto.replace('ñ', 'n').replace('Ñ', 'N')
        texto = texto.replace('_', '').replace('-', '')
        return texto.upper()
    return texto

# UDF registrada para Spark
udf_limpieza = udf(limpiar_texto, StringType())
