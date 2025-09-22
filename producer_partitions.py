#!/usr/bin/env python3
"""
Producer simple pour l'exercice 4 - Test des consumer groups
"""

import json
import time
import sys
from kafka import KafkaProducer
from datetime import datetime
from random import choice

def main():
    if len(sys.argv) != 2:
        print("Usage: python producer_partitions.py <topic_name>")
        print("Exemple: python producer_partitions.py weather_partitioned")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    
    print(f"Producer pour topic partitionné: {topic_name}")
    print("Envoi de 20 messages avec clés différentes pour distribution sur partitions")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8')
        )
    except Exception as e:
        print(f"Erreur de connexion Kafka: {e}")
        sys.exit(1)
    
    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Bordeaux", "Lille", "Nantes"]
    
    try:
        for i in range(20):
            city = choice(cities)
            
            # Données météo simulées
            weather_data = {
                "city": city,
                "timestamp": datetime.now().isoformat(),
                "temperature": choice(range(10, 30)),
                "humidity": choice(range(40, 90)),
                "wind_speed": choice(range(5, 25)),
                "message_id": i + 1
            }
            
            # Utiliser la ville comme clé pour distribuer sur les partitions
            key = city
            
            future = producer.send(topic_name, key=key, value=weather_data)
            record_metadata = future.get(timeout=10)
            
            print(f"Message {i+1}/20 - {city} → Partition {record_metadata.partition}, Offset {record_metadata.offset}")
            
            time.sleep(0.5)  # Pause courte entre messages
        
        print(f"\n20 messages envoyés vers {topic_name}")
        print("Lancez maintenant les consumer groups pour voir la distribution!")
        
    except KeyboardInterrupt:
        print("\nArrêt du producer...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()