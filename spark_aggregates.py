#!/usr/bin/env python3
"""
Exercice 5 : Agrégats en temps réel avec Spark Streaming
Calcule des métriques sur des fenêtres glissantes depuis weather_transformed
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

def create_spark_session():
    """Crée une session Spark configurée pour Kafka et streaming"""
    return SparkSession.builder \
        .appName("WeatherAggregates") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def define_weather_transformed_schema():
    """Définit le schéma des données weather_transformed"""
    return StructType([
        StructField("city", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("message_id", IntegerType(), True),
        StructField("wind_alert_level", StringType(), True),
        StructField("heat_alert_level", StringType(), True)
    ])

def process_weather_aggregates(spark):
    """Traite les agrégats en temps réel avec fenêtres glissantes"""
    
    # Schema des données transformées
    weather_schema = define_weather_transformed_schema()
    
    # Lecture du stream Kafka weather_transformed
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weather_transformed") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parsing du JSON avec timestamp
    weather_df = df.select(
        from_json(col("value").cast("string"), weather_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Conversion du timestamp pour les fenêtres
    weather_df = weather_df.withColumn(
        "event_timestamp", 
        to_timestamp(col("event_time"))
    )
    
    # === FENÊTRE GLISSANTE 1 MINUTE ===
    # Agrégats d'alertes par type et niveau
    alerts_1min = weather_df \
        .withWatermark("event_timestamp", "30 seconds") \
        .groupBy(
            window(col("event_timestamp"), "1 minute", "30 seconds"),
            col("city")
        ) \
        .agg(
            # Alertes vent
            sum(when(col("wind_alert_level") == "level_1", 1).otherwise(0)).alias("wind_level_1_count"),
            sum(when(col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("wind_level_2_count"),
            # Alertes chaleur
            sum(when(col("heat_alert_level") == "level_1", 1).otherwise(0)).alias("heat_level_1_count"),
            sum(when(col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("heat_level_2_count"),
            # Stats température
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            # Total messages
            count("*").alias("total_messages")
        ) \
        .withColumn("window_duration", lit("1_minute"))
    
    # === FENÊTRE GLISSANTE 5 MINUTES ===
    alerts_5min = weather_df \
        .withWatermark("event_timestamp", "1 minute") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("city")
        ) \
        .agg(
            # Alertes vent
            sum(when(col("wind_alert_level") == "level_1", 1).otherwise(0)).alias("wind_level_1_count"),
            sum(when(col("wind_alert_level") == "level_2", 1).otherwise(0)).alias("wind_level_2_count"),
            # Alertes chaleur
            sum(when(col("heat_alert_level") == "level_1", 1).otherwise(0)).alias("heat_level_1_count"),
            sum(when(col("heat_alert_level") == "level_2", 1).otherwise(0)).alias("heat_level_2_count"),
            # Stats température
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            # Total messages
            count("*").alias("total_messages")
        ) \
        .withColumn("window_duration", lit("5_minutes"))
    
    # Union des deux fenêtres
    all_aggregates = alerts_1min.union(alerts_5min)
    
    # Ajout de métadonnées
    final_aggregates = all_aggregates.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("window_duration"),
        col("city"),
        col("wind_level_1_count"),
        col("wind_level_2_count"),
        col("heat_level_1_count"),
        col("heat_level_2_count"),
        round(col("avg_temperature"), 2).alias("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        col("total_messages"),
        current_timestamp().alias("computed_at")
    )
    
    # Sortie console pour debug
    query_console = final_aggregates.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return query_console

def main():
    print("Démarrage des agrégats météo en temps réel")
    print("Lecture de weather_transformed → Calcul fenêtres glissantes")
    
    try:
        # Création de la session Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        print("Session Spark créée")
        print("Configuration des fenêtres glissantes :")
        print("  - Fenêtre 1 minute (sliding 30s)")
        print("  - Fenêtre 5 minutes (sliding 1min)")
        print("En attente de messages sur weather_transformed...")
        
        # Traitement des agrégats
        query = process_weather_aggregates(spark)
        
        print("Calculateur d'agrégats démarré")
        print("Appuyez sur Ctrl+C pour arrêter")
        
        # Attendre l'arrêt
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nArrêt du calculateur d'agrégats...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Session Spark fermée")

if __name__ == "__main__":
    main()