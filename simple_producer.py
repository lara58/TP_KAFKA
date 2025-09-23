#!/usr/bin/env python3
"""
Producteur simple pour données météo historiques
Récupère les données et les envoie vers Kafka
"""

import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import argparse

# Coordonnées des villes
CITIES = {
    'paris': {'lat': 48.8566, 'lon': 2.3522, 'country': 'france'},
    'london': {'lat': 51.5074, 'lon': -0.1278, 'country': 'uk'},
    'berlin': {'lat': 52.5200, 'lon': 13.4050, 'country': 'germany'}
}

def get_weather_data(city, year, month):
    """Récupère les données météo d'un mois"""
    coords = CITIES[city.lower()]
    
    # Dates du mois
    start_date = f"{year}-{month:02d}-01"
    if month == 12:
        end_date = f"{year + 1}-01-01"
    else:
        end_date = f"{year}-{month + 1:02d}-01"
    
    params = {
        'latitude': coords['lat'],
        'longitude': coords['lon'],
        'start_date': start_date,
        'end_date': end_date,
        'hourly': 'temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation',
        'timezone': 'Europe/Paris'
    }
    
    print(f"Requête API: {city} {year}/{month:02d}")
    
    response = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params)
    response.raise_for_status()
    
    data = response.json()
    
    # Message pour Kafka
    message = {
        'city': city.lower(),
        'country': coords['country'],
        'year': year,
        'month': month,
        'timestamp': datetime.now().isoformat(),
        'data': data.get('hourly', {})
    }
    
    print(f"   {len(data.get('hourly', {}).get('time', []))} points récupérés")
    return message

def main():
    parser = argparse.ArgumentParser(description='Producteur météo historique')
    parser.add_argument('--city', default='paris', choices=['paris', 'london', 'berlin'], help='Ville')
    parser.add_argument('--start-year', type=int, default=2014, help='Année de début')
    parser.add_argument('--end-year', type=int, default=2024, help='Année de fin')
    parser.add_argument('--topic', default='weather_history', help='Topic Kafka')
    
    args = parser.parse_args()
    
    print(f"Démarrage producteur: {args.city} {args.start_year}-{args.end_year}")
    print("=" * 50)
    
    # Connexion Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connecté à Kafka")
    except Exception as e:
        print(f"Erreur Kafka: {e}")
        print("Lancez: docker-compose up -d")
        return
    
    # Traitement multi-années
    total_months = 0
    success = 0
    
    for year in range(args.start_year, args.end_year + 1):
        print(f"\nTraitement année {year}:")
        
        for month in range(1, 13):
            total_months += 1
            try:
                # Récupérer les données
                message = get_weather_data(args.city, year, month)
                
                # Envoyer à Kafka
                future = producer.send(args.topic, value=message)
                future.get(timeout=10)
                
                print(f"   Mois {month:02d}: {len(message['data'].get('time', []))} points -> Kafka")
                success += 1
                
                time.sleep(0.2)  # Pause entre requêtes
                
            except Exception as e:
                print(f"   Erreur mois {month}: {e}")
    
    producer.close()
    print(f"\nTerminé: {success}/{total_months} mois traités")

if __name__ == "__main__":
    main()