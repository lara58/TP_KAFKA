#!/usr/bin/env python3
"""
Exercice 4 PDF : Version simplifiée du processeur d'alertes météo
Alternative pour éviter les problèmes Hadoop/Windows
"""

import time
import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

def calculate_wind_alert_level(wind_speed):
    """Calcule le niveau d'alerte vent"""
    if wind_speed < 10:
        return "level_0"
    elif wind_speed < 20:
        return "level_1"
    else:
        return "level_2"

def calculate_heat_alert_level(temperature):
    """Calcule le niveau d'alerte chaleur"""
    if temperature < 25:
        return "level_0"
    elif temperature < 35:
        return "level_1"
    else:
        return "level_2"

def process_weather_messages():
    """Traite les messages météo et génère les alertes"""
    
    print("Démarrage du processeur d'alertes météo (version simplifiée)")
    print("Lecture de weather_stream → Transformation → weather_transformed")
    
    # Configuration consumer
    consumer = KafkaConsumer(
        'weather_stream',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='weather_alert_processor'
    )
    
    # Configuration producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("Processeur d'alertes démarré")
    print("En attente de messages sur weather_stream...")
    print("Règles d'alertes:")
    print("  Vent: <10 m/s=level_0, 10-20 m/s=level_1, >20 m/s=level_2")
    print("  Chaleur: <25°C=level_0, 25-35°C=level_1, >35°C=level_2")
    print("Appuyez sur Ctrl+C pour arrêter\n")
    
    try:
        for message in consumer:
            weather_data = message.value
            
            # Extraction des données
            city = weather_data.get('city', 'Unknown')
            temperature = weather_data.get('temperature', 0.0)
            wind_speed = weather_data.get('wind_speed', 0.0)
            humidity = weather_data.get('humidity', 0.0)
            message_id = weather_data.get('message_id', 0)
            
            # Calcul des alertes
            wind_alert = calculate_wind_alert_level(wind_speed)
            heat_alert = calculate_heat_alert_level(temperature)
            
            # Création du message transformé
            transformed_data = {
                'city': city,
                'event_time': datetime.now().isoformat(),
                'temperature': temperature,
                'wind_speed': wind_speed,
                'humidity': humidity,
                'message_id': message_id,
                'wind_alert_level': wind_alert,
                'heat_alert_level': heat_alert
            }
            
            # Envoi vers weather_transformed
            producer.send('weather_transformed', value=transformed_data)
            producer.flush()
            
            # Affichage du traitement
            print(f"OK {city}: {temperature}°C, {wind_speed} m/s")
            print(f"   -> Alertes: Vent={wind_alert}, Chaleur={heat_alert}")
            
    except KeyboardInterrupt:
        print("\nArrêt du processeur d'alertes...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        consumer.close()
        producer.close()
        print("Processeur fermé")

if __name__ == "__main__":
    process_weather_messages()