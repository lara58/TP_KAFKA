#!/usr/bin/env python3
"""
Producer Kafka pour l'exercice 3
Envoie des données météo vers un topic Kafka
"""

import json
import time
import sys
import requests
from kafka import KafkaProducer
from datetime import datetime
from random import choice

def get_weather_data(city="Paris"):
    """
    Récupère les données météo pour une ville donnée
    Utilise l'API Open-Meteo avec des coordonnées prédéfinies
    """
    # Coordonnées de quelques villes françaises
    cities = {
        "Paris": {"lat": 48.8566, "lon": 2.3522},
        "Lyon": {"lat": 45.7640, "lon": 4.8357},
        "Marseille": {"lat": 43.2965, "lon": 5.3698},
        "Toulouse": {"lat": 43.6047, "lon": 1.4442},
        "Nice": {"lat": 43.7102, "lon": 7.2620}
    }
    
    if city not in cities:
        city = "Paris"  # Valeur par défaut
    
    try:
        lat, lon = cities[city]["lat"], cities[city]["lon"]
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "Europe/Paris"
        }
        
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        # Formatage simple des données
        weather_data = {
            "city": city,
            "timestamp": datetime.now().isoformat(),
            "temperature": data["current"]["temperature_2m"],
            "humidity": data["current"]["relative_humidity_2m"],
            "wind_speed": data["current"]["wind_speed_10m"]
        }
        
        return weather_data
        
    except Exception as e:
        print(f"Erreur API météo: {e}")
        # Données simulées en cas d'erreur
        return {
            "city": city,
            "timestamp": datetime.now().isoformat(),
            "temperature": choice(range(10, 25)),
            "humidity": choice(range(40, 80)),
            "wind_speed": choice(range(5, 20))
        }

def main():
    if len(sys.argv) != 2:
        print("Usage: python producer.py <topic_name>")
        print("Exemple: python producer.py weather_stream")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    
    print(f"Démarrage du producer météo pour le topic: {topic_name}")
    print("Envoi de 10 messages météo...")
    
    # Configuration du producer (similaire au consumer)
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )
    except Exception as e:
        print(f"Erreur de connexion au serveur Kafka: {e}")
        print("Assurez-vous que Kafka est démarré (docker-compose up)")
        sys.exit(1)
    
    try:
        cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]
        
        for i in range(10):
            # Choisir une ville aléatoirement
            city = choice(cities)
            
            # Récupérer les données météo
            weather_data = get_weather_data(city)
            weather_data["message_id"] = i + 1
            
            # Envoyer le message
            future = producer.send(topic_name, value=weather_data)
            
            # Attendre la confirmation
            try:
                record_metadata = future.get(timeout=10)
                print(f"Message {i+1}/10 envoyé - {city}: {weather_data['temperature']}°C")
                print(f"  Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            except Exception as e:
                print(f"Erreur envoi message {i+1}: {e}")
            
            # Petite pause entre les messages
            time.sleep(1)
        
        print("\nTous les messages ont été envoyés!")
        print(f"Vous pouvez maintenant lancer le consumer pour les lire:")
        print(f"python consumer.py {topic_name}")
        
    except KeyboardInterrupt:
        print("\nArrêt du producer...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()