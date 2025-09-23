#!/usr/bin/env python3
"""
Producteur de test pour l'exercice 7 - Génère des données avec alertes
"""

import json
import sys
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_extreme_weather_message(city, country, temp, humidity, wind, lat=0.0, lon=0.0):
    """Crée un message avec des conditions météo extrêmes"""
    return {
        "city": city,
        "country": country,
        "latitude": lat,
        "longitude": lon,
        "temperature": temp,
        "humidity": humidity,
        "wind_speed": wind,
        "timestamp": datetime.now().isoformat(),
        "message_id": 1
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_producer.py <topic>")
        sys.exit(1)
    
    topic = sys.argv[1]
    
    # Producer Kafka
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print(f"Envoi de messages de test avec conditions extrêmes vers {topic}...")
    
    # Messages de test avec conditions extrêmes
    test_messages = [
        # Température très élevée
        create_extreme_weather_message("Dubai", "UAE", 45.5, 65, 15, 25.2048, 55.2708),
        # Température très basse
        create_extreme_weather_message("Yakutsk", "Russia", -25.3, 75, 20, 62.0341, 129.6752), 
        # Humidité très élevée
        create_extreme_weather_message("Singapore", "Singapore", 30.0, 95, 10, 1.3521, 103.8198),
        # Vent très fort
        create_extreme_weather_message("Chicago", "USA", 15.0, 70, 65, 41.8781, -87.6298),
        # Conditions normales (pas d'alerte)
        create_extreme_weather_message("Paris", "France", 18.0, 60, 12, 48.8566, 2.3522)
    ]
    
    for i, message in enumerate(test_messages, 1):
        try:
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10)
            
            print(f"Message {i}: {message['city']} - T:{message['temperature']}°C, H:{message['humidity']}%, V:{message['wind_speed']}km/h")
            print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi du message {i}: {e}")
    
    producer.flush()
    producer.close()
    print(f"\nTous les messages de test envoyés vers {topic}!")

if __name__ == "__main__":
    main()