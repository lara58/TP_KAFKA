#!/usr/bin/env python3
"""
Exercice 10 : Détection des records climatiques (Version sans Spark)
Analyse directe des données HDFS pour détecter les records météorologiques
"""

import json
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging
from collections import defaultdict

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WeatherRecordsAnalyzer:
    """Analyseur de records climatiques sans Spark"""
    
    def __init__(self, hdfs_base_path="./hdfs-data"):
        self.hdfs_base_path = Path(hdfs_base_path)
        logger.info("Analyseur de records climatiques initialisé (Mode direct)")
    
    def load_weather_data_for_city(self, country, city):
        """Charge toutes les données météo d'une ville depuis HDFS"""
        city_path = self.hdfs_base_path / country / city / "weather_history"
        
        if not city_path.exists():
            logger.warning(f"Aucune donnée trouvée pour {city}, {country}")
            return []
        
        json_files = list(city_path.glob("*.json"))
        if not json_files:
            logger.warning(f"Aucun fichier JSON pour {city}, {country}")
            return []
        
        logger.info(f"Chargement de {len(json_files)} fichiers pour {city}, {country}")
        
        all_data = []
        
        for json_file in sorted(json_files):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extraire les données météo
                weather_data = data.get('data', {})
                times = weather_data.get('time', [])
                temperatures = weather_data.get('temperature_2m', [])
                humidity = weather_data.get('relative_humidity_2m', [])
                wind_speeds = weather_data.get('wind_speed_10m', [])
                precipitation = weather_data.get('precipitation', [])
                
                # Créer les enregistrements
                for i, time_str in enumerate(times):
                    if i < len(temperatures) and i < len(wind_speeds) and i < len(precipitation):
                        # Vérifier que les valeurs sont valides
                        temp = temperatures[i]
                        wind = wind_speeds[i]
                        precip = precipitation[i]
                        humid = humidity[i] if i < len(humidity) else None
                        
                        if temp is not None and wind is not None and precip is not None:
                            record = {
                                'city': city,
                                'country': country,
                                'datetime': time_str,
                                'date': time_str.split('T')[0],  # Extraire la date
                                'year': data.get('year'),
                                'month': data.get('month'),
                                'temperature': float(temp),
                                'humidity': float(humid) if humid is not None else None,
                                'wind_speed': float(wind),
                                'precipitation': float(precip)
                            }
                            all_data.append(record)
                
            except Exception as e:
                logger.error(f"Erreur lecture fichier {json_file}: {e}")
        
        logger.info(f"Données chargées: {len(all_data)} enregistrements valides pour {city}")
        return all_data
    
    def find_temperature_records(self, data):
        """Trouve les records de température"""
        if not data:
            return None
        
        # Grouper par jour pour les températures min/max quotidiennes
        daily_temps = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'temps': []})
        
        for record in data:
            date = record['date']
            temp = record['temperature']
            daily_temps[date]['temps'].append(record)
            
            if temp < daily_temps[date]['min']:
                daily_temps[date]['min'] = temp
            if temp > daily_temps[date]['max']:
                daily_temps[date]['max'] = temp
        
        # Trouver le jour le plus chaud et le plus froid
        hottest_day = None
        coldest_day = None
        max_temp = float('-inf')
        min_temp = float('inf')
        
        for date, day_data in daily_temps.items():
            if day_data['max'] > max_temp:
                max_temp = day_data['max']
                hottest_day = {'date': date, 'temperature': day_data['max']}
                # Trouver l'heure exacte
                hottest_record = max(day_data['temps'], key=lambda x: x['temperature'])
                hottest_day['exact_datetime'] = hottest_record['datetime']
            
            if day_data['min'] < min_temp:
                min_temp = day_data['min']
                coldest_day = {'date': date, 'temperature': day_data['min']}
                # Trouver l'heure exacte
                coldest_record = min(day_data['temps'], key=lambda x: x['temperature'])
                coldest_day['exact_datetime'] = coldest_record['datetime']
        
        return {
            "hottest_day": hottest_day,
            "coldest_day": coldest_day
        }
    
    def find_wind_records(self, data):
        """Trouve la rafale de vent la plus forte"""
        if not data:
            return None
        
        max_wind_record = max(data, key=lambda x: x['wind_speed'])
        
        return {
            "strongest_wind": {
                "datetime": max_wind_record['datetime'],
                "date": max_wind_record['date'],
                "wind_speed": max_wind_record['wind_speed'],
                "wind_speed_kmh": max_wind_record['wind_speed'] * 3.6
            }
        }
    
    def find_precipitation_records(self, data):
        """Trouve les records de précipitations"""
        if not data:
            return None
        
        # Précipitations par jour
        daily_rain = defaultdict(float)
        for record in data:
            daily_rain[record['date']] += record['precipitation']
        
        rainiest_date = max(daily_rain.keys(), key=lambda d: daily_rain[d])
        
        # Précipitations par mois
        monthly_rain = defaultdict(float)
        for record in data:
            month_key = f"{record['year']}-{record['month']:02d}"
            monthly_rain[month_key] += record['precipitation']
        
        rainiest_month_key = max(monthly_rain.keys(), key=lambda m: monthly_rain[m])
        year, month = rainiest_month_key.split('-')
        
        # L'heure avec le plus de précipitations
        max_hourly_record = max(data, key=lambda x: x['precipitation'])
        
        return {
            "rainiest_day": {
                "date": rainiest_date,
                "precipitation": daily_rain[rainiest_date]
            },
            "rainiest_month": {
                "year": int(year),
                "month": int(month),
                "precipitation": monthly_rain[rainiest_month_key]
            },
            "max_hourly_rain": {
                "datetime": max_hourly_record['datetime'],
                "precipitation": max_hourly_record['precipitation']
            }
        }
    
    def analyze_city_records(self, country, city):
        """Analyse complète des records pour une ville"""
        logger.info(f"Analyse des records climatiques pour {city}, {country}")
        
        # Charger les données
        data = self.load_weather_data_for_city(country, city)
        if not data:
            return None
        
        # Calculer les statistiques générales
        total_records = len(data)
        dates = [record['date'] for record in data]
        start_date = min(dates)
        end_date = max(dates)
        years = list(set(record['year'] for record in data))
        
        # Trouver les records
        temp_records = self.find_temperature_records(data)
        wind_records = self.find_wind_records(data)
        rain_records = self.find_precipitation_records(data)
        
        # Compilation des résultats
        city_records = {
            "city": city,
            "country": country,
            "analysis_timestamp": datetime.now().isoformat(),
            "data_summary": {
                "total_measurements": total_records,
                "period_start": start_date,
                "period_end": end_date,
                "years_analyzed": len(years)
            },
            "temperature_records": temp_records,
            "wind_records": wind_records,
            "precipitation_records": rain_records
        }
        
        logger.info(f"Records calculés pour {city}: "
                   f"Temp max: {temp_records['hottest_day']['temperature']:.1f}°C, "
                   f"Temp min: {temp_records['coldest_day']['temperature']:.1f}°C, "
                   f"Vent max: {wind_records['strongest_wind']['wind_speed']:.1f} m/s")
        
        return city_records
    
    def find_all_cities(self):
        """Trouve toutes les villes avec des données météo dans HDFS"""
        cities = []
        
        if not self.hdfs_base_path.exists():
            logger.warning(f"Répertoire HDFS inexistant: {self.hdfs_base_path}")
            return cities
        
        for country_dir in self.hdfs_base_path.iterdir():
            if country_dir.is_dir():
                for city_dir in country_dir.iterdir():
                    if city_dir.is_dir():
                        weather_dir = city_dir / "weather_history"
                        if weather_dir.exists() and list(weather_dir.glob("*.json")):
                            cities.append((country_dir.name, city_dir.name))
        
        logger.info(f"Villes trouvées avec données météo: {cities}")
        return cities

def main():
    """Fonction principale"""
    print("EXERCICE 10 : DÉTECTION DES RECORDS CLIMATIQUES")
    print("=" * 60)
    print("Analyse directe des données HDFS (Sans Spark)")
    
    try:
        # Initialiser l'analyseur
        analyzer = WeatherRecordsAnalyzer()
        
        # Trouver toutes les villes
        cities = analyzer.find_all_cities()
        
        if not cities:
            print("Aucune ville trouvée avec des données météo")
            return
        
        print(f"\nAnalyse de {len(cities)} villes...")
        
        # Analyser chaque ville
        all_records = []
        
        for country, city in cities:
            print(f"\nAnalyse: {city.title()}, {country.title()}")
            print("-" * 40)
            
            records = analyzer.analyze_city_records(country, city)
            if records:
                all_records.append(records)
                
                # Afficher les résultats
                temp = records["temperature_records"]
                wind = records["wind_records"]
                rain = records["precipitation_records"]
                
                print(f"[ANALYSE] Période analysée: {records['data_summary']['period_start']} "
                      f"à {records['data_summary']['period_end']}")
                print(f"[DONNÉES] Mesures totales: {records['data_summary']['total_measurements']:,}")
                print(f"[TEMPÉRATURE] Jour le plus chaud: {temp['hottest_day']['temperature']:.1f}°C "
                      f"le {temp['hottest_day']['date']}")
                print(f"[TEMPÉRATURE] Jour le plus froid: {temp['coldest_day']['temperature']:.1f}°C "
                      f"le {temp['coldest_day']['date']}")
                print(f"[VENT] Vent le plus fort: {wind['strongest_wind']['wind_speed']:.1f} m/s "
                      f"({wind['strongest_wind']['wind_speed_kmh']:.1f} km/h) "
                      f"le {wind['strongest_wind']['date']}")
                print(f"[PRÉCIPITATIONS] Jour le plus pluvieux: {rain['rainiest_day']['precipitation']:.1f} mm "
                      f"le {rain['rainiest_day']['date']}")
        
        print(f"\n[SUCCÈS] Analyse terminée pour {len(all_records)} villes")
        return all_records
        
    except Exception as e:
        logger.error(f"Erreur dans l'analyse: {e}")
        raise

if __name__ == "__main__":
    records = main()