#!/usr/bin/env python3
"""
Script simple pour tester l'API Open-Meteo Archive
"""

import requests
import json
from datetime import datetime

def test_api():
    """Test simple de l'API Archive"""
    print("Test API Open-Meteo Archive")
    print("=" * 30)
    
    # Test avec Paris, juin 2023
    params = {
        'latitude': 48.8566,
        'longitude': 2.3522,
        'start_date': '2023-06-01',
        'end_date': '2023-06-30',
        'hourly': 'temperature_2m,wind_speed_10m',
        'timezone': 'Europe/Paris'
    }
    
    try:
        response = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params)
        response.raise_for_status()
        
        data = response.json()
        hourly = data.get('hourly', {})
        temps = hourly.get('temperature_2m', [])
        
        print(f"Succès!")
        print(f"   Points de données: {len(temps)}")
        print(f"   Température moyenne: {sum(temps)/len(temps):.1f}°C")
        print(f"   Première mesure: {hourly.get('time', ['N/A'])[0]}")
        
        return True
        
    except Exception as e:
        print(f"Erreur: {e}")
        return False

if __name__ == "__main__":
    test_api()