#!/usr/bin/env python3
"""
Consumer Kafka pour l'exercice 6
Lit et affiche les messages enrichis avec informations géographiques
"""

import json
import sys
from kafka import KafkaConsumer

def main():
    if len(sys.argv) != 2:
        print("Usage: python consumer.py <topic_name>")
        print("Exemple: python consumer.py weather_stream")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    
    print(f"Consumer démarré pour le topic: {topic_name}")
    print("En attente de messages... (Ctrl+C pour arrêter)")
    print("-" * 60)
    
    # Configuration du consumer
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        print(f"Erreur de connexion au serveur Kafka: {e}")
        print("Assurez-vous que Kafka est démarré (docker-compose up)")
        sys.exit(1)
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            data = message.value
            
            # Affichage structuré des données enrichies
            print(f"Message {message_count}")
            print(f"  Ville: {data.get('city', 'N/A')}")
            print(f"  Pays: {data.get('country', 'N/A')}")
            print(f"  Coordonnees: {data.get('latitude', 'N/A')}, {data.get('longitude', 'N/A')}")
            print(f"  Temperature: {data.get('temperature', 'N/A')}°C")
            print(f"  Humidite: {data.get('humidity', 'N/A')}%")
            print(f"  Vent: {data.get('wind_speed', 'N/A')} km/h")
            print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
            print(f"  Partition: {message.partition}, Offset: {message.offset}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print(f"\nConsumer arrêté. {message_count} messages reçus.")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()