#!/usr/bin/env python3
"""
Exercice 11 : Simulation complète climatologie urbaine
Test complet de l'analyse des profils saisonniers sans serveur Kafka
"""

import json
import time
from pathlib import Path
from datetime import datetime
from queue import Queue
from threading import Thread
from urban_climatology_analyzer import UrbanClimatologyAnalyzer
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockKafkaMessage:
    """Message Kafka simulé"""
    def __init__(self, key, value, partition=0, offset=0):
        self.key = key
        self.value = value
        self.partition = partition
        self.offset = offset

class SimulatedClimatologyProducer:
    """Producteur simulé pour les profils climatologiques"""
    
    def __init__(self, message_queue):
        self.message_queue = message_queue
        self.messages_sent = 0
        
    def send_city_climatology(self, climatology_data):
        """Simule l'envoi du profil climatologique d'une ville"""
        if not climatology_data:
            return False
        
        try:
            city = climatology_data['city']
            country = climatology_data['country']
            key = f"{country}_{city}"
            
            message = {
                **climatology_data,
                "message_type": "seasonal_climatology",
                "emission_timestamp": datetime.now().isoformat(),
                "source": "urban_climatology_analyzer_simulation"
            }
            
            mock_message = MockKafkaMessage(
                key=key,
                value=message,
                partition=0,
                offset=self.messages_sent
            )
            
            self.message_queue.put(mock_message)
            self.messages_sent += 1
            
            logger.info(f"Message climatologique simulé ajouté pour {city}, {country}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur simulation envoi pour {city}: {e}")
            return False
    
    def send_global_climatology_summary(self, all_analyses):
        """Simule l'envoi de la synthèse climatologique globale"""
        if not all_analyses:
            return False
        
        try:
            total_cities = len(all_analyses)
            countries = list(set(a['country'] for a in all_analyses))
            
            # Trouver les villes avec les extrêmes
            hottest_city_analysis = max(all_analyses, 
                key=lambda x: max(month['temperature']['average'] 
                    for month in x['seasonal_profile'].values() 
                    if not month.get('no_data', False)))
            
            coldest_city_analysis = min(all_analyses,
                key=lambda x: min(month['temperature']['average'] 
                    for month in x['seasonal_profile'].values() 
                    if not month.get('no_data', False)))
            
            windiest_city_analysis = max(all_analyses,
                key=lambda x: max(month['wind_speed']['average'] 
                    for month in x['seasonal_profile'].values() 
                    if not month.get('no_data', False)))
            
            # Calculer moyennes globales
            all_temps = []
            all_winds = []
            all_precips = []
            
            for analysis in all_analyses:
                for month_data in analysis['seasonal_profile'].values():
                    if not month_data.get('no_data', False):
                        all_temps.append(month_data['temperature']['average'])
                        all_winds.append(month_data['wind_speed']['average'])
                        all_precips.append(month_data['precipitation']['total'])
            
            global_summary = {
                "message_type": "global_climatology_summary",
                "emission_timestamp": datetime.now().isoformat(),
                "source": "urban_climatology_analyzer_simulation",
                "analysis_summary": {
                    "total_cities_analyzed": total_cities,
                    "countries": countries,
                    "total_measurements": sum(a['data_summary']['total_measurements'] for a in all_analyses),
                    "analysis_period": {
                        "start_year": min(int(a['data_summary']['period'].split('-')[0]) for a in all_analyses),
                        "end_year": max(int(a['data_summary']['period'].split('-')[1]) for a in all_analyses)
                    }
                },
                "global_climate_extremes": {
                    "hottest_monthly_average": {
                        "temperature": max(all_temps),
                        "city": hottest_city_analysis['city'],
                        "country": hottest_city_analysis['country']
                    },
                    "coldest_monthly_average": {
                        "temperature": min(all_temps),
                        "city": coldest_city_analysis['city'],
                        "country": coldest_city_analysis['country']
                    },
                    "windiest_monthly_average": {
                        "wind_speed": max(all_winds),
                        "city": windiest_city_analysis['city'],
                        "country": windiest_city_analysis['country']
                    }
                },
                "global_averages": {
                    "temperature": round(sum(all_temps) / len(all_temps), 2),
                    "wind_speed": round(sum(all_winds) / len(all_winds), 2),
                    "monthly_precipitation": round(sum(all_precips) / len(all_precips), 2)
                },
                "climate_patterns": {
                    "temperature_variability": round(max(all_temps) - min(all_temps), 2),
                    "cities_with_extreme_alerts": len([a for a in all_analyses 
                        if a['annual_statistics']['alert_statistics']['level_2_percent'] > 5.0])
                }
            }
            
            mock_message = MockKafkaMessage(
                key="GLOBAL_CLIMATOLOGY",
                value=global_summary,
                partition=0,
                offset=self.messages_sent
            )
            
            self.message_queue.put(mock_message)
            self.messages_sent += 1
            
            logger.info("Message de synthèse climatologique globale ajouté")
            
            # Afficher synthèse
            print("\nSYNTHÈSE CLIMATOLOGIQUE GLOBALE (SIMULATION)")
            print("=" * 70)
            print(f"Villes analysées: {total_cities} dans {len(countries)} pays")
            print(f"Mesures totales: {global_summary['analysis_summary']['total_measurements']:,}")
            print(f"Période: {global_summary['analysis_summary']['analysis_period']['start_year']}-"
                  f"{global_summary['analysis_summary']['analysis_period']['end_year']}")
            
            extremes = global_summary['global_climate_extremes']
            print(f"Température mensuelle max: {extremes['hottest_monthly_average']['temperature']:.1f}°C "
                  f"à {extremes['hottest_monthly_average']['city'].title()}")
            print(f"Température mensuelle min: {extremes['coldest_monthly_average']['temperature']:.1f}°C "
                  f"à {extremes['coldest_monthly_average']['city'].title()}")
            print(f"Vent mensuel max: {extremes['windiest_monthly_average']['wind_speed']:.1f} m/s "
                  f"à {extremes['windiest_monthly_average']['city'].title()}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur simulation synthèse climatologique: {e}")
            return False

class SimulatedClimatologyConsumer:
    """Consumer simulé pour la sauvegarde des profils"""
    
    def __init__(self, message_queue, hdfs_base_path="./hdfs-data"):
        self.message_queue = message_queue
        self.hdfs_base_path = Path(hdfs_base_path)
        self.profiles_processed = 0
        self.files_saved = 0
    
    def save_city_climatology(self, climatology_data):
        """Sauvegarde le profil climatologique d'une ville"""
        try:
            country = climatology_data['country']
            city = climatology_data['city']
            
            # Créer le répertoire selon les spécifications de l'exercice
            profile_dir = self.hdfs_base_path / country / city / "seasonal_profile"
            profile_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"seasonal_profile_{timestamp}.json"
            filepath = profile_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(climatology_data, f, indent=2, ensure_ascii=False)
            
            self.files_saved += 1
            
            summary = climatology_data["climate_summary"]
            annual = climatology_data["annual_statistics"]
            
            print(f"\nSauvegardé: {city.title()}, {country.title()}")
            print(f"   Mois le plus chaud: {summary['hottest_month']['name']} "
                  f"({summary['hottest_month']['average_temperature']:.1f}°C)")
            print(f"   Mois le plus froid: {summary['coldest_month']['name']} "
                  f"({summary['coldest_month']['average_temperature']:.1f}°C)")
            print(f"   Mois le plus venteux: {summary['windiest_month']['name']} "
                  f"({summary['windiest_month']['average_wind_speed']:.1f} m/s)")
            print(f"   Alertes niveau 1: {annual['alert_statistics']['level_1_percent']:.1f}%")
            print(f"   Alertes niveau 2: {annual['alert_statistics']['level_2_percent']:.1f}%")
            print(f"   Fichier: {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde simulée {city}: {e}")
            return False
    
    def save_global_climatology_summary(self, summary):
        """Sauvegarde la synthèse climatologique globale"""
        try:
            summary_dir = self.hdfs_base_path / "global_summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"global_climatology_summary_{timestamp}.json"
            filepath = summary_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            self.files_saved += 1
            
            analysis = summary["analysis_summary"]
            extremes = summary["global_climate_extremes"]
            averages = summary["global_averages"]
            
            print(f"\nSYNTHÈSE CLIMATOLOGIQUE GLOBALE SAUVEGARDÉE")
            print(f"   Villes: {analysis['total_cities_analyzed']}, "
                  f"Pays: {len(analysis['countries'])}")
            print(f"   Mesures: {analysis['total_measurements']:,}")
            print(f"   Période: {analysis['analysis_period']['start_year']}-"
                  f"{analysis['analysis_period']['end_year']}")
            print(f"   Temp max: {extremes['hottest_monthly_average']['temperature']:.1f}°C "
                  f"à {extremes['hottest_monthly_average']['city'].title()}")
            print(f"   Temp min: {extremes['coldest_monthly_average']['temperature']:.1f}°C "
                  f"à {extremes['coldest_monthly_average']['city'].title()}")
            print(f"   Fichier: {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde synthèse simulée: {e}")
            return False
    
    def process_messages(self):
        """Traite tous les messages de la queue"""
        while not self.message_queue.empty():
            try:
                message = self.message_queue.get(timeout=1)
                
                if message.value and 'message_type' in message.value:
                    message_type = message.value['message_type']
                    
                    if message_type == 'seasonal_climatology':
                        success = self.save_city_climatology(message.value)
                    elif message_type == 'global_climatology_summary':
                        success = self.save_global_climatology_summary(message.value)
                    else:
                        success = False
                    
                    if success:
                        self.profiles_processed += 1
                        print(f"Message traité (Offset: {message.offset})")
                    else:
                        print(f"Échec traitement (Offset: {message.offset})")
                
                self.message_queue.task_done()
                
            except Exception as e:
                logger.error(f"Erreur traitement message simulé: {e}")
                break

def producer_thread(analyzer, producer, cities):
    """Thread producteur pour l'analyse climatologique"""
    print("Thread Producteur : Analyse climatologique et émission")
    
    all_analyses = []
    
    for country, city in cities:
        print(f"\nAnalyse climatologique: {city.title()}, {country.title()}")
        
        # Analyser la climatologie
        analysis = analyzer.analyze_city_climatology(country, city)
        if analysis:
            all_analyses.append(analysis)
            
            # Émettre vers la queue
            if producer.send_city_climatology(analysis):
                print(f"Profil climatologique émis pour {city}")
            
            time.sleep(0.1)
    
    # Émettre la synthèse globale
    if all_analyses:
        print(f"\nÉmission synthèse climatologique globale...")
        producer.send_global_climatology_summary(all_analyses)
    
    print(f"\nProducteur terminé : {len(all_analyses)} villes traitées")

def consumer_thread(consumer):
    """Thread consumer pour la sauvegarde"""
    print("Thread Consumer : Traitement et sauvegarde")
    
    time.sleep(2)  # Attendre le producteur
    consumer.process_messages()
    
    print(f"\nConsumer terminé : {consumer.profiles_processed} messages traités")

def run_climatology_simulation():
    """Lance la simulation complète de l'exercice 11"""
    print("EXERCICE 11 : SIMULATION COMPLÈTE")
    print("CLIMATOLOGIE URBAINE - PROFILS SAISONNIERS")
    print("=" * 80)
    print("Mode simulation - Analyse directe des patterns climatiques")
    
    try:
        # Initialiser l'analyseur
        print("\n1. Initialisation de l'analyseur climatologique...")
        analyzer = UrbanClimatologyAnalyzer()
        
        # Trouver les villes
        cities = analyzer.find_all_cities()
        if not cities:
            print("Aucune ville trouvée avec des données météo")
            return
        
        print(f"   {len(cities)} villes trouvées : {cities}")
        
        # Initialiser la queue de messages
        message_queue = Queue()
        
        # Initialiser producteur et consumer simulés
        print("\n2. Initialisation des composants simulés...")
        producer = SimulatedClimatologyProducer(message_queue)
        consumer = SimulatedClimatologyConsumer(message_queue)
        
        # Lancer les threads
        print("\n3. Lancement de l'analyse climatologique...")
        
        producer_t = Thread(
            target=producer_thread,
            args=(analyzer, producer, cities),
            name="ClimatologyProducer"
        )
        
        consumer_t = Thread(
            target=consumer_thread,
            args=(consumer,),
            name="ClimatologyConsumer"
        )
        
        # Démarrer les threads
        producer_t.start()
        consumer_t.start()
        
        # Attendre la fin
        producer_t.join()
        consumer_t.join()
        
        # Statistiques finales
        print(f"\nRÉSULTATS DE LA SIMULATION")
        print("=" * 50)
        print(f"Villes analysées: {len(cities)}")
        print(f"Messages émis: {producer.messages_sent}")
        print(f"Messages traités: {consumer.profiles_processed}")
        print(f"Fichiers sauvegardés: {consumer.files_saved}")
        
        # Vérifier les fichiers créés
        profile_dirs = list(Path("./hdfs-data").glob("*/*/seasonal_profile"))
        summaries_dir = Path("./hdfs-data/global_summaries")
        
        print(f"\nStructure HDFS créée:")
        for profile_dir in profile_dirs:
            files = list(profile_dir.glob("*.json"))
            print(f"   {profile_dir}: {len(files)} profil(s) saisonnier(s)")
        
        if summaries_dir.exists():
            summary_files = list(summaries_dir.glob("*climatology*.json"))
            print(f"   {summaries_dir}: {len(summary_files)} synthèse(s) climatologique(s)")
        
        print("\nExercice 11 - Climatologie urbaine terminé avec succès !")
        
    except Exception as e:
        logger.error(f"Erreur simulation climatologique: {e}")
        print(f"Erreur: {e}")
        raise

def main():
    """Fonction principale"""
    try:
        run_climatology_simulation()
    except KeyboardInterrupt:
        print("\nSimulation interrompue par l'utilisateur")
    except Exception as e:
        print(f"\nErreur fatale: {e}")

if __name__ == "__main__":
    main()