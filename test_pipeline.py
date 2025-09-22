#!/usr/bin/env python3
"""
Script de test de la pipeline complète
Exercice 5 - TP Kafka
"""

import time
import json
from kafka import KafkaConsumer, KafkaProducer
from threading import Thread

def monitor_topic(topic_name, label):
    """Monitore un topic Kafka"""
    print(f"\nMonitoring {topic_name} ({label})")
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    message_count = 0
    try:
        for message in consumer:
            message_count += 1
            timestamp = time.strftime("%H:%M:%S")
            print(f"[{timestamp}] {label} #{message_count}: {message.value}")
            
            if message_count >= 10:  # Limite pour le test
                break
                
    except KeyboardInterrupt:
        print(f"\nMonitoring {topic_name} arrêté")
    except Exception as e:
        print(f"Erreur monitoring {topic_name}: {e}")

def test_pipeline():
    """Test la pipeline complète"""
    print("Test de la pipeline Exercice 5")
    print("=" * 50)
    
    # Lancer les monitors en parallèle
    threads = []
    
    # Monitor weather_stream
    t1 = Thread(target=monitor_topic, args=("weather_stream", "Données météo"))
    t1.daemon = True
    t1.start()
    threads.append(t1)
    
    # Monitor weather_transformed  
    t2 = Thread(target=monitor_topic, args=("weather_transformed", "Alertes"))
    t2.daemon = True
    t2.start()
    threads.append(t2)
    
    # Attendre et laisser tourner
    print("\nMonitoring en cours... (Ctrl+C pour arrêter)")
    try:
        time.sleep(60)  # Monitor pendant 1 minute
    except KeyboardInterrupt:
        print("\nTest arrêté par l'utilisateur")
    
    print("\nTest terminé")

if __name__ == "__main__":
    test_pipeline()