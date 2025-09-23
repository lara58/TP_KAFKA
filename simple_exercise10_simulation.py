#!/usr/bin/env python3
"""
Exercice 10 : Simulation complète (Version simplifiée sans Spark)
Test complet de détection des records climatiques
"""

import json
import time
from pathlib import Path
from datetime import datetime
from queue import Queue
from threading import Thread
from simple_records_analyzer import WeatherRecordsAnalyzer
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

class SimpleWeatherRecordsProducer:
    """Producteur simulé pour les records climatiques"""
    
    def __init__(self, message_queue):
        self.message_queue = message_queue
        self.messages_sent = 0
        
    def send_city_records(self, city_records):
        """Simule l'envoi des records d'une ville"""
        if not city_records:
            return False
        
        try:
            city = city_records['city']
            country = city_records['country']
            key = f"{country}_{city}"
            
            message = {
                **city_records,
                "message_type": "weather_records",
                "emission_timestamp": datetime.now().isoformat(),
                "source": "simple_records_analyzer"
            }
            
            mock_message = MockKafkaMessage(
                key=key,
                value=message,
                partition=0,
                offset=self.messages_sent
            )
            
            self.message_queue.put(mock_message)
            self.messages_sent += 1
            
            logger.info(f"Message simulé ajouté pour {city}, {country}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur simulation envoi pour {city}: {e}")
            return False
    
    def send_summary_record(self, all_records):
        """Simule l'envoi de la synthèse globale"""
        if not all_records:
            return False
        
        try:
            total_cities = len(all_records)
            countries = list(set(r['country'] for r in all_records))
            
            # Trouver les records globaux
            all_temps_max = [r['temperature_records']['hottest_day']['temperature'] 
                           for r in all_records]
            all_temps_min = [r['temperature_records']['coldest_day']['temperature'] 
                           for r in all_records]
            all_winds = [r['wind_records']['strongest_wind']['wind_speed'] 
                        for r in all_records]
            
            global_hottest = max(all_temps_max)
            global_coldest = min(all_temps_min)
            global_strongest_wind = max(all_winds)
            
            # Trouver les villes avec ces records
            hottest_city_data = next(r for r in all_records 
                                   if r['temperature_records']['hottest_day']['temperature'] == global_hottest)
            coldest_city_data = next(r for r in all_records 
                                   if r['temperature_records']['coldest_day']['temperature'] == global_coldest)
            windiest_city_data = next(r for r in all_records 
                                    if r['wind_records']['strongest_wind']['wind_speed'] == global_strongest_wind)
            
            summary = {
                "message_type": "weather_records_summary",
                "emission_timestamp": datetime.now().isoformat(),
                "source": "simple_records_analyzer",
                "analysis_summary": {
                    "total_cities_analyzed": total_cities,
                    "countries": countries,
                    "total_measurements": sum(r['data_summary']['total_measurements'] for r in all_records)
                },
                "global_records": {
                    "hottest_temperature": {
                        "value": global_hottest,
                        "city": hottest_city_data['city'],
                        "country": hottest_city_data['country'],
                        "date": hottest_city_data['temperature_records']['hottest_day']['date']
                    },
                    "coldest_temperature": {
                        "value": global_coldest,
                        "city": coldest_city_data['city'],
                        "country": coldest_city_data['country'],
                        "date": coldest_city_data['temperature_records']['coldest_day']['date']
                    },
                    "strongest_wind": {
                        "value": global_strongest_wind,
                        "value_kmh": global_strongest_wind * 3.6,
                        "city": windiest_city_data['city'],
                        "country": windiest_city_data['country'],
                        "date": windiest_city_data['wind_records']['strongest_wind']['date']
                    }
                }
            }
            
            mock_message = MockKafkaMessage(
                key="GLOBAL_SUMMARY",
                value=summary,
                partition=0,
                offset=self.messages_sent
            )
            
            self.message_queue.put(mock_message)
            self.messages_sent += 1
            
            logger.info("Message de synthèse globale ajouté")
            
            # Afficher synthèse
            print("\nSYNTHÈSE GLOBALE DES RECORDS")
            print("=" * 60)
            print(f"🌍 {total_cities} villes analysées dans {len(countries)} pays")
            print(f"📊 {summary['analysis_summary']['total_measurements']:,} mesures totales")
            print(f"🔥 Record de chaleur: {global_hottest:.1f}°C à {hottest_city_data['city'].title()}")
            print(f"🥶 Record de froid: {global_coldest:.1f}°C à {coldest_city_data['city'].title()}")
            print(f"💨 Record de vent: {global_strongest_wind:.1f} m/s à {windiest_city_data['city'].title()}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur simulation synthèse: {e}")
            return False

class SimpleWeatherRecordsConsumer:
    """Consumer simulé pour la sauvegarde des records"""
    
    def __init__(self, message_queue, hdfs_base_path="./hdfs-data"):
        self.message_queue = message_queue
        self.hdfs_base_path = Path(hdfs_base_path)
        self.records_processed = 0
        self.files_saved = 0
    
    def save_city_records(self, city_records):
        """Sauvegarde les records d'une ville"""
        try:
            country = city_records['country']
            city = city_records['city']
            
            records_dir = self.hdfs_base_path / country / city / "weather_records"
            records_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"records_{timestamp}.json"
            filepath = records_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(city_records, f, indent=2, ensure_ascii=False)
            
            self.files_saved += 1
            
            temp = city_records["temperature_records"]
            wind = city_records["wind_records"]
            
            print(f"\n📁 Sauvegardé: {city.title()}, {country.title()}")
            print(f"   🌡️  Max: {temp['hottest_day']['temperature']:.1f}°C "
                  f"({temp['hottest_day']['date']})")
            print(f"   🥶 Min: {temp['coldest_day']['temperature']:.1f}°C "
                  f"({temp['coldest_day']['date']})")
            print(f"   💨 Vent: {wind['strongest_wind']['wind_speed']:.1f} m/s "
                  f"({wind['strongest_wind']['date']})")
            print(f"   📂 {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde simulée {city}: {e}")
            return False
    
    def save_global_summary(self, summary):
        """Sauvegarde la synthèse globale"""
        try:
            summary_dir = self.hdfs_base_path / "global_summaries"
            summary_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"global_records_summary_{timestamp}.json"
            filepath = summary_dir / filename
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            self.files_saved += 1
            
            analysis = summary["analysis_summary"]
            records = summary["global_records"]
            
            print(f"\n📊 SYNTHÈSE GLOBALE SAUVEGARDÉE")
            print(f"   🌍 {analysis['total_cities_analyzed']} villes, "
                  f"{len(analysis['countries'])} pays")
            print(f"   📈 {analysis['total_measurements']:,} mesures totales")
            print(f"   🔥 Record chaleur: {records['hottest_temperature']['value']:.1f}°C "
                  f"à {records['hottest_temperature']['city'].title()}")
            print(f"   🥶 Record froid: {records['coldest_temperature']['value']:.1f}°C "
                  f"à {records['coldest_temperature']['city'].title()}")
            print(f"   💨 Record vent: {records['strongest_wind']['value']:.1f} m/s "
                  f"à {records['strongest_wind']['city'].title()}")
            print(f"   📂 {filename}")
            
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
                    
                    if message_type == 'weather_records':
                        success = self.save_city_records(message.value)
                    elif message_type == 'weather_records_summary':
                        success = self.save_global_summary(message.value)
                    else:
                        success = False
                    
                    if success:
                        self.records_processed += 1
                        print(f"✅ Message traité (Offset: {message.offset})")
                    else:
                        print(f"❌ Échec traitement (Offset: {message.offset})")
                
                self.message_queue.task_done()
                
            except Exception as e:
                logger.error(f"Erreur traitement message simulé: {e}")
                break

def producer_thread(analyzer, producer, cities):
    """Thread producteur pour l'analyse et l'émission"""
    print("🔄 Thread Producteur : Analyse des records et émission")
    
    all_records = []
    
    for country, city in cities:
        print(f"\n🔍 Analyse: {city.title()}, {country.title()}")
        
        records = analyzer.analyze_city_records(country, city)
        if records:
            all_records.append(records)
            
            if producer.send_city_records(records):
                print(f"📤 Records émis pour {city}")
            
            time.sleep(0.1)
    
    if all_records:
        print(f"\n📤 Émission synthèse globale...")
        producer.send_summary_record(all_records)
    
    print(f"\n✅ Producteur terminé : {len(all_records)} villes traitées")

def consumer_thread(consumer):
    """Thread consumer pour la sauvegarde"""
    print("🔄 Thread Consumer : Traitement et sauvegarde")
    
    time.sleep(2)  # Attendre le producteur
    consumer.process_messages()
    
    print(f"\n✅ Consumer terminé : {consumer.records_processed} messages traités")

def run_simple_simulation():
    """Lance la simulation simplifiée de l'exercice 10"""
    print("EXERCICE 10 : SIMULATION COMPLÈTE (VERSION SIMPLIFIÉE)")
    print("DÉTECTION DES RECORDS CLIMATIQUES")
    print("=" * 80)
    print("Mode simulation - Analyse directe sans Spark")
    
    try:
        # Initialiser l'analyseur
        print("\n1️⃣  Initialisation de l'analyseur...")
        analyzer = WeatherRecordsAnalyzer()
        
        # Trouver les villes
        cities = analyzer.find_all_cities()
        if not cities:
            print("❌ Aucune ville trouvée avec des données météo")
            return
        
        print(f"✅ {len(cities)} villes trouvées : {cities}")
        
        # Initialiser la queue de messages
        message_queue = Queue()
        
        # Initialiser producteur et consumer simulés
        print("\n2️⃣  Initialisation des composants simulés...")
        producer = SimpleWeatherRecordsProducer(message_queue)
        consumer = SimpleWeatherRecordsConsumer(message_queue)
        
        # Lancer les threads
        print("\n3️⃣  Lancement de l'analyse et du traitement...")
        
        producer_t = Thread(
            target=producer_thread,
            args=(analyzer, producer, cities),
            name="Producer"
        )
        
        consumer_t = Thread(
            target=consumer_thread,
            args=(consumer,),
            name="Consumer"
        )
        
        # Démarrer les threads
        producer_t.start()
        consumer_t.start()
        
        # Attendre la fin
        producer_t.join()
        consumer_t.join()
        
        # Statistiques finales
        print(f"\n📊 RÉSULTATS DE LA SIMULATION")
        print("=" * 50)
        print(f"🏙️  Villes analysées: {len(cities)}")
        print(f"📤 Messages émis: {producer.messages_sent}")
        print(f"📥 Messages traités: {consumer.records_processed}")
        print(f"📁 Fichiers sauvegardés: {consumer.files_saved}")
        
        # Vérifier les fichiers créés
        records_dirs = list(Path("./hdfs-data").glob("*/*/weather_records"))
        summaries_dir = Path("./hdfs-data/global_summaries")
        
        print(f"\n📂 Structure HDFS créée:")
        for records_dir in records_dirs:
            files = list(records_dir.glob("*.json"))
            print(f"   {records_dir}: {len(files)} fichier(s)")
        
        if summaries_dir.exists():
            summary_files = list(summaries_dir.glob("*.json"))
            print(f"   {summaries_dir}: {len(summary_files)} synthèse(s)")
        
        print("\n✨ Exercice 10 terminé avec succès !")
        
    except Exception as e:
        logger.error(f"Erreur simulation: {e}")
        print(f"❌ Erreur: {e}")
        raise

def main():
    """Fonction principale"""
    try:
        run_simple_simulation()
    except KeyboardInterrupt:
        print("\n⏹️  Simulation interrompue par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")

if __name__ == "__main__":
    main()