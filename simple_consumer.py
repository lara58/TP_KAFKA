#!/usr/bin/env python3
"""
Consumer simple pour sauvegarder les données météo dans HDFS
Lit depuis Kafka et organise les fichiers par pays/ville
"""

import json
import os
from pathlib import Path
from kafka import KafkaConsumer
from datetime import datetime
import argparse

def save_to_hdfs(message, base_path="./hdfs-data"):
    """Sauvegarde un message dans la structure HDFS"""
    country = message['country']
    city = message['city']
    year = message['year']
    month = message['month']
    
    # Créer le répertoire
    hdfs_path = Path(base_path) / country / city / "weather_history"
    hdfs_path.mkdir(parents=True, exist_ok=True)
    
    # Nom du fichier
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{year}_{month:02d}_{timestamp}.json"
    
    # Sauvegarder
    file_path = hdfs_path / filename
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(message, f, indent=2, ensure_ascii=False)
    
    return file_path

def main():
    parser = argparse.ArgumentParser(description='Consumer météo historique')
    parser.add_argument('--topic', default='weather_history', help='Topic Kafka')
    parser.add_argument('--group', default='hdfs_group', help='Consumer group')
    
    args = parser.parse_args()
    
    print(f"Démarrage consumer HDFS")
    print(f"Topic: {args.topic}")
    print("=" * 30)
    
    try:
        # Connexion Kafka
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=['localhost:9092'],
            group_id=args.group,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        print("Connecté à Kafka")
        
        # Traitement des messages
        message_count = 0
        
        for message in consumer:
            try:
                data = message.value
                
                # Sauvegarder en HDFS
                file_path = save_to_hdfs(data)
                message_count += 1
                
                # Afficher le progrès
                city = data.get('city', 'unknown')
                year = data.get('year', 'unknown')
                month = data.get('month', 'unknown')
                data_points = len(data.get('data', {}).get('time', []))
                
                print(f"[{message_count}] {city} {year}/{month:02d} -> {file_path.name}")
                print(f"   {data_points} points, {file_path.stat().st_size} bytes")
                
            except Exception as e:
                print(f"Erreur traitement message: {e}")
        
    except KeyboardInterrupt:
        print(f"\nArrêt manuel")
    except Exception as e:
        print(f"Erreur consumer: {e}")
        print("Vérifiez que Kafka est démarré: docker-compose up -d")
    finally:
        if 'consumer' in locals():
            consumer.close()
        print(f"Messages traités: {message_count}")

if __name__ == "__main__":
    main()