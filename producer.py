#!/usr/bin/env python3
"""
Producer Kafka pour l'exercice 6
Envoie des données météo géolocalisées vers un topic Kafka
Accepte ville et pays en arguments
"""

import json
import time
import sys
import requests
from kafka import KafkaProducer
from datetime import datetime

def get_coordinates(city, country=None):
    """
    Récupère les coordonnées géographiques d'une ville via l'API Geocoding
    """
    try:
        url = "https://geocoding-api.open-meteo.com/v1/search"
        params = {
            "name": city,
            "count": 1,
            "language": "en",
            "format": "json"
        }
        
        if country:
            params["country"] = country
            
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("results"):
            raise ValueError(f"Ville '{city}' non trouvée")
            
        result = data["results"][0]
        return {
            "latitude": result["latitude"],
            "longitude": result["longitude"], 
            "country": result["country"],
            "city": result["name"]
        }
        
    except Exception as e:
        print(f"Erreur géocodage: {e}")
        raise

def get_weather_data(latitude, longitude, city, country):
    """
    Récupère les données météo pour des coordonnées données
    """
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "auto"
        }
        
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        # Formatage des données enrichies
        weather_data = {
            "city": city,
            "country": country,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": datetime.now().isoformat(),
            "temperature": data["current"]["temperature_2m"],
            "humidity": data["current"]["relative_humidity_2m"],
            "wind_speed": data["current"]["wind_speed_10m"]
        }
        
        return weather_data
        
    except Exception as e:
        print(f"Erreur API météo: {e}")
        # Données simulées en cas d'erreur
        import random
        return {
            "city": city,
            "country": country,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": datetime.now().isoformat(),
            "temperature": random.randint(10, 25),
            "humidity": random.randint(40, 80),
            "wind_speed": random.randint(5, 20)
        }

def main():
    # Vérification des arguments
    if len(sys.argv) < 3:
        print("Usage: python producer.py <topic_name> <city> [country]")
        print("Exemples:")
        print("  python producer.py weather_stream Paris France")
        print("  python producer.py weather_stream Tokyo Japan")
        print("  python producer.py weather_stream Berlin")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    city = sys.argv[2]
    country = sys.argv[3] if len(sys.argv) > 3 else None
    
    print(f"Démarrage du producer météo pour le topic: {topic_name}")
    print(f"Ville: {city}" + (f", Pays: {country}" if country else ""))
    
    # Étape 1: Géocodage
    try:
        print("Récupération des coordonnées...")
        coords = get_coordinates(city, country)
        print(f"Coordonnées trouvées: {coords['city']}, {coords['country']}")
        print(f"Latitude: {coords['latitude']}, Longitude: {coords['longitude']}")
    except Exception as e:
        print(f"Erreur lors du géocodage: {e}")
        sys.exit(1)
    
    # Configuration du producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )
    except Exception as e:
        print(f"Erreur de connexion au serveur Kafka: {e}")
        print("Assurez-vous que Kafka est démarré (docker-compose up)")
        sys.exit(1)
    
    print("Envoi de 10 messages météo...")
    
    try:
        for i in range(10):
            # Récupérer les données météo
            weather_data = get_weather_data(
                coords['latitude'], 
                coords['longitude'],
                coords['city'], 
                coords['country']
            )
            weather_data["message_id"] = i + 1
            
            # Envoyer le message
            future = producer.send(topic_name, value=weather_data)
            
            # Attendre la confirmation
            try:
                record_metadata = future.get(timeout=10)
                print(f"Message {i+1}/10 envoyé - {coords['city']}: {weather_data['temperature']}°C")
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