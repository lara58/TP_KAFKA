#!/usr/bin/env python3
"""
Exercice 4 PDF : Transformation des données et détection d'alertes avec Spark
Traite le flux weather_stream en temps réel et produit weather_transformed avec alertes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Crée une session Spark configurée pour Kafka"""
    return SparkSession.builder \
        .appName("WeatherAlertProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def define_weather_schema():
    """Définit le schéma des données météo de weather_stream"""
    return StructType([
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("message_id", IntegerType(), True)
    ])

def calculate_wind_alert(wind_speed):
    """Calcule le niveau d'alerte vent"""
    return when(wind_speed < 10, "level_0") \
           .when(wind_speed < 20, "level_1") \
           .otherwise("level_2")

def calculate_heat_alert(temperature):
    """Calcule le niveau d'alerte chaleur"""
    return when(temperature < 25, "level_0") \
           .when(temperature < 35, "level_1") \
           .otherwise("level_2")

def process_weather_stream(spark):
    """Traite le flux weather_stream et génère weather_transformed"""
    
    # Schema des données météo
    weather_schema = define_weather_schema()
    
    # Lecture du stream Kafka weather_stream
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_stream") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parsing du JSON
    weather_df = df.select(
        from_json(col("value").cast("string"), weather_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Ajout des alertes et transformations
    transformed_df = weather_df.select(
        col("city"),
        current_timestamp().alias("event_time"),
        col("temperature"),
        col("wind_speed"),
        col("humidity"),
        col("message_id"),
        calculate_wind_alert(col("wind_speed")).alias("wind_alert_level"),
        calculate_heat_alert(col("temperature")).alias("heat_alert_level")
    )
    
    # Conversion en JSON pour Kafka
    output_df = transformed_df.select(
        to_json(struct("*")).alias("value")
    )
    
    # Écriture vers weather_transformed
    query = output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "weather_transformed") \
        .option("checkpointLocation", "./checkpoint") \
        .outputMode("append") \
        .start()
    
    return query

def main():
    print("Démarrage du processeur d'alertes météo Spark")
    print("Lecture de weather_stream → Transformation → weather_transformed")
    
    try:
        # Création de la session Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")  # Réduire les logs
        
        print("Session Spark créée")
        print("En attente de messages sur weather_stream...")
        
        # Traitement du stream
        query = process_weather_stream(spark)
        
        print("Processeur d'alertes démarré")
        print("Les alertes sont envoyées vers weather_transformed")
        print("Appuyez sur Ctrl+C pour arrêter")
        
        # Attendre l'arrêt
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nArrêt du processeur d'alertes...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Session Spark fermée")

if __name__ == "__main__":
    main()