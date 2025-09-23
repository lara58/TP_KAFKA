#!/usr/bin/env python3
"""
Exercice 11 : Climatologie urbaine - Profils saisonniers
Analyse Spark pour calculer les profils climatiques mensuels par ville
"""

import json
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UrbanClimatologyAnalyzer:
    """Analyseur de climatologie urbaine pour profils saisonniers"""
    
    def __init__(self, hdfs_base_path="./hdfs-data"):
        self.hdfs_base_path = Path(hdfs_base_path)
        logger.info("Analyseur de climatologie urbaine initialisé")
    
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
                        temp = temperatures[i]
                        wind = wind_speeds[i]
                        precip = precipitation[i]
                        humid = humidity[i] if i < len(humidity) else None
                        
                        if temp is not None and wind is not None and precip is not None:
                            record = {
                                'city': city,
                                'country': country,
                                'datetime': time_str,
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
        
        logger.info(f"Données chargées: {len(all_data)} enregistrements pour {city}")
        return all_data
    
    def calculate_alert_level(self, temperature, wind_speed, precipitation):
        """Calcule le niveau d'alerte météorologique basé sur les seuils"""
        alert_level = "normal"
        
        # Seuils d'alerte (adaptés au climat européen)
        if (temperature > 35.0 or temperature < -5.0 or 
            wind_speed > 15.0 or precipitation > 10.0):
            alert_level = "level_1"
        
        if (temperature > 40.0 or temperature < -10.0 or 
            wind_speed > 25.0 or precipitation > 20.0):
            alert_level = "level_2"
        
        return alert_level
    
    def calculate_seasonal_profile(self, data):
        """Calcule le profil saisonnier pour une ville"""
        if not data:
            return None
        
        # Grouper par mois
        monthly_data = defaultdict(list)
        monthly_alerts = defaultdict(lambda: {"level_1": 0, "level_2": 0, "normal": 0, "total": 0})
        
        for record in data:
            month = record['month']
            monthly_data[month].append(record)
            
            # Calculer le niveau d'alerte pour cet enregistrement
            alert_level = self.calculate_alert_level(
                record['temperature'], 
                record['wind_speed'], 
                record['precipitation']
            )
            monthly_alerts[month][alert_level] += 1
            monthly_alerts[month]["total"] += 1
        
        # Calculer les moyennes mensuelles
        seasonal_profile = {}
        
        for month in range(1, 13):  # Mois 1-12
            if month in monthly_data:
                month_records = monthly_data[month]
                
                # Moyennes
                avg_temp = sum(r['temperature'] for r in month_records) / len(month_records)
                avg_wind = sum(r['wind_speed'] for r in month_records) / len(month_records)
                avg_precipitation = sum(r['precipitation'] for r in month_records) / len(month_records)
                
                # Humidité (si disponible)
                humidity_values = [r['humidity'] for r in month_records if r['humidity'] is not None]
                avg_humidity = sum(humidity_values) / len(humidity_values) if humidity_values else None
                
                # Probabilités d'alerte
                alerts = monthly_alerts[month]
                prob_level_1 = (alerts["level_1"] / alerts["total"]) * 100 if alerts["total"] > 0 else 0
                prob_level_2 = (alerts["level_2"] / alerts["total"]) * 100 if alerts["total"] > 0 else 0
                
                # Nom du mois
                month_names = {
                    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril",
                    5: "Mai", 6: "Juin", 7: "Juillet", 8: "Août",
                    9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre"
                }
                
                seasonal_profile[month] = {
                    "month_number": month,
                    "month_name": month_names[month],
                    "measurements_count": len(month_records),
                    "temperature": {
                        "average": round(avg_temp, 2),
                        "min": round(min(r['temperature'] for r in month_records), 2),
                        "max": round(max(r['temperature'] for r in month_records), 2)
                    },
                    "wind_speed": {
                        "average": round(avg_wind, 2),
                        "max": round(max(r['wind_speed'] for r in month_records), 2)
                    },
                    "precipitation": {
                        "average": round(avg_precipitation, 2),
                        "total": round(sum(r['precipitation'] for r in month_records), 2),
                        "max_daily": round(max(r['precipitation'] for r in month_records), 2)
                    },
                    "humidity": {
                        "average": round(avg_humidity, 2) if avg_humidity is not None else None
                    },
                    "alert_probabilities": {
                        "level_1_percent": round(prob_level_1, 2),
                        "level_2_percent": round(prob_level_2, 2),
                        "normal_percent": round(100 - prob_level_1 - prob_level_2, 2)
                    },
                    "alert_counts": {
                        "level_1": alerts["level_1"],
                        "level_2": alerts["level_2"],
                        "normal": alerts["normal"],
                        "total": alerts["total"]
                    }
                }
            else:
                # Mois sans données
                seasonal_profile[month] = {
                    "month_number": month,
                    "month_name": month_names.get(month, f"Mois {month}"),
                    "measurements_count": 0,
                    "no_data": True
                }
        
        return seasonal_profile
    
    def analyze_city_climatology(self, country, city):
        """Analyse climatologique complète d'une ville"""
        logger.info(f"Analyse climatologique pour {city}, {country}")
        
        # Charger les données
        data = self.load_weather_data_for_city(country, city)
        if not data:
            return None
        
        # Calculer le profil saisonnier
        seasonal_profile = self.calculate_seasonal_profile(data)
        
        # Statistiques générales
        total_measurements = len(data)
        years = list(set(record['year'] for record in data))
        start_year = min(years)
        end_year = max(years)
        
        # Compilation des résultats
        climatology_analysis = {
            "city": city,
            "country": country,
            "analysis_timestamp": datetime.now().isoformat(),
            "analysis_type": "seasonal_climatology",
            "data_summary": {
                "total_measurements": total_measurements,
                "years_analyzed": len(years),
                "period": f"{start_year}-{end_year}",
                "years_list": sorted(years)
            },
            "seasonal_profile": seasonal_profile,
            "annual_statistics": self.calculate_annual_statistics(data),
            "climate_summary": self.generate_climate_summary(seasonal_profile)
        }
        
        logger.info(f"Profil saisonnier calculé pour {city}: {len(seasonal_profile)} mois analysés")
        return climatology_analysis
    
    def calculate_annual_statistics(self, data):
        """Calcule les statistiques annuelles"""
        if not data:
            return None
        
        temperatures = [r['temperature'] for r in data]
        wind_speeds = [r['wind_speed'] for r in data]
        precipitations = [r['precipitation'] for r in data]
        
        # Compter les alertes
        alert_counts = {"level_1": 0, "level_2": 0, "normal": 0}
        for record in data:
            alert_level = self.calculate_alert_level(
                record['temperature'], record['wind_speed'], record['precipitation']
            )
            alert_counts[alert_level] += 1
        
        total_alerts = sum(alert_counts.values())
        
        return {
            "temperature": {
                "annual_average": round(sum(temperatures) / len(temperatures), 2),
                "absolute_min": round(min(temperatures), 2),
                "absolute_max": round(max(temperatures), 2),
                "range": round(max(temperatures) - min(temperatures), 2)
            },
            "wind_speed": {
                "annual_average": round(sum(wind_speeds) / len(wind_speeds), 2),
                "max_recorded": round(max(wind_speeds), 2)
            },
            "precipitation": {
                "annual_total": round(sum(precipitations), 2),
                "annual_average": round(sum(precipitations) / len(precipitations), 4),
                "max_hourly": round(max(precipitations), 2)
            },
            "alert_statistics": {
                "total_measurements": total_alerts,
                "level_1_count": alert_counts["level_1"],
                "level_2_count": alert_counts["level_2"],
                "normal_count": alert_counts["normal"],
                "level_1_percent": round((alert_counts["level_1"] / total_alerts) * 100, 2),
                "level_2_percent": round((alert_counts["level_2"] / total_alerts) * 100, 2)
            }
        }
    
    def generate_climate_summary(self, seasonal_profile):
        """Génère un résumé climatique basé sur le profil saisonnier"""
        if not seasonal_profile:
            return None
        
        # Trouver les mois avec températures extrêmes
        valid_months = {k: v for k, v in seasonal_profile.items() if not v.get('no_data', False)}
        
        if not valid_months:
            return None
        
        # Mois le plus chaud et le plus froid
        hottest_month = max(valid_months.values(), key=lambda x: x['temperature']['average'])
        coldest_month = min(valid_months.values(), key=lambda x: x['temperature']['average'])
        
        # Mois le plus venteux
        windiest_month = max(valid_months.values(), key=lambda x: x['wind_speed']['average'])
        
        # Mois le plus pluvieux
        rainiest_month = max(valid_months.values(), key=lambda x: x['precipitation']['total'])
        
        # Mois avec plus d'alertes
        most_alerts_month = max(valid_months.values(), 
                               key=lambda x: x['alert_probabilities']['level_1_percent'] + 
                                            x['alert_probabilities']['level_2_percent'])
        
        return {
            "hottest_month": {
                "name": hottest_month['month_name'],
                "average_temperature": hottest_month['temperature']['average']
            },
            "coldest_month": {
                "name": coldest_month['month_name'],
                "average_temperature": coldest_month['temperature']['average']
            },
            "windiest_month": {
                "name": windiest_month['month_name'],
                "average_wind_speed": windiest_month['wind_speed']['average']
            },
            "rainiest_month": {
                "name": rainiest_month['month_name'],
                "total_precipitation": rainiest_month['precipitation']['total']
            },
            "most_alerts_month": {
                "name": most_alerts_month['month_name'],
                "alert_probability": round(
                    most_alerts_month['alert_probabilities']['level_1_percent'] + 
                    most_alerts_month['alert_probabilities']['level_2_percent'], 2
                )
            },
            "seasonal_patterns": {
                "temperature_range": round(
                    hottest_month['temperature']['average'] - coldest_month['temperature']['average'], 2
                ),
                "months_with_data": len(valid_months)
            }
        }
    
    def save_seasonal_profile(self, country, city, climatology_data):
        """Sauvegarde le profil saisonnier dans HDFS"""
        try:
            # Créer le répertoire de destination
            profile_dir = self.hdfs_base_path / country / city / "seasonal_profile"
            profile_dir.mkdir(parents=True, exist_ok=True)
            
            # Nom du fichier avec timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"seasonal_profile_{timestamp}.json"
            filepath = profile_dir / filename
            
            # Sauvegarder le fichier
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(climatology_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Profil saisonnier sauvegardé: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde profil {city}: {e}")
            return None
    
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
        
        logger.info(f"Villes trouvées pour analyse climatologique: {cities}")
        return cities

def main():
    """Fonction principale"""
    print("EXERCICE 11 : CLIMATOLOGIE URBAINE - PROFILS SAISONNIERS")
    print("=" * 70)
    print("Analyse des patterns climatiques mensuels par ville")
    
    try:
        # Initialiser l'analyseur
        analyzer = UrbanClimatologyAnalyzer()
        
        # Trouver toutes les villes
        cities = analyzer.find_all_cities()
        
        if not cities:
            print("Aucune ville trouvée avec des données météo")
            return
        
        print(f"\nAnalyse climatologique de {len(cities)} villes...")
        
        all_analyses = []
        
        for country, city in cities:
            print(f"\nAnalyse: {city.title()}, {country.title()}")
            print("-" * 50)
            
            # Analyser la climatologie
            analysis = analyzer.analyze_city_climatology(country, city)
            if analysis:
                all_analyses.append(analysis)
                
                # Sauvegarder le profil
                filepath = analyzer.save_seasonal_profile(country, city, analysis)
                
                # Afficher le résumé
                summary = analysis["climate_summary"]
                annual = analysis["annual_statistics"]
                
                print(f"Période analysée: {analysis['data_summary']['period']}")
                print(f"Mesures totales: {analysis['data_summary']['total_measurements']:,}")
                print(f"Années: {analysis['data_summary']['years_analyzed']} ans")
                
                print(f"\nPROFIL CLIMATIQUE:")
                print(f"  Mois le plus chaud: {summary['hottest_month']['name']} "
                      f"({summary['hottest_month']['average_temperature']:.1f}°C)")
                print(f"  Mois le plus froid: {summary['coldest_month']['name']} "
                      f"({summary['coldest_month']['average_temperature']:.1f}°C)")
                print(f"  Mois le plus venteux: {summary['windiest_month']['name']} "
                      f"({summary['windiest_month']['average_wind_speed']:.1f} m/s)")
                print(f"  Mois le plus pluvieux: {summary['rainiest_month']['name']} "
                      f"({summary['rainiest_month']['total_precipitation']:.1f} mm)")
                
                print(f"\nSTATISTIQUES ANNUELLES:")
                print(f"  Température moyenne: {annual['temperature']['annual_average']:.1f}°C")
                print(f"  Amplitude thermique: {annual['temperature']['range']:.1f}°C")
                print(f"  Vent moyen: {annual['wind_speed']['annual_average']:.1f} m/s")
                print(f"  Précipitations totales: {annual['precipitation']['annual_total']:.1f} mm/an")
                
                print(f"\nALERTES MÉTÉOROLOGIQUES:")
                print(f"  Niveau 1: {annual['alert_statistics']['level_1_percent']:.1f}% du temps")
                print(f"  Niveau 2: {annual['alert_statistics']['level_2_percent']:.1f}% du temps")
                
                if filepath:
                    print(f"\nFichier sauvegardé: {filepath.name}")
        
        print(f"\nAnalyse climatologique terminée pour {len(all_analyses)} villes")
        return all_analyses
        
    except Exception as e:
        logger.error(f"Erreur dans l'analyse climatologique: {e}")
        raise

if __name__ == "__main__":
    analyses = main()