#!/usr/bin/env python3
"""
Simulation Pipeline 10 ans avec logique Kafka
Mode local sans serveur Kafka pour démonstration
"""

import requests
import json
import time
from datetime import datetime
from pathlib import Path
import threading
import queue

class MockKafkaProducer:
    """Simulateur de producteur Kafka"""
    def __init__(self, topic_queue):
        self.topic_queue = topic_queue
        
    def send(self, topic, value):
        self.topic_queue.put(value)
        return MockFuture()
        
    def close(self):
        pass

class MockFuture:
    """Simulateur de future Kafka"""
    def get(self, timeout=10):
        return True

class MockKafkaConsumer:
    """Simulateur de consumer Kafka"""
    def __init__(self, topic_queue):
        self.topic_queue = topic_queue
        self.running = True
        
    def consume(self):
        while self.running:
            try:
                message = self.topic_queue.get(timeout=1)
                yield MockMessage(message)
            except queue.Empty:
                continue
                
    def close(self):
        self.running = False

class MockMessage:
    """Simulateur de message Kafka"""
    def __init__(self, value):
        self.value = value

def producer_worker(topic_queue, start_year, end_year, city='paris'):
    """Thread producteur - récupère les données et les envoie vers la queue"""
    print(f"PRODUCTEUR: Démarrage pour {city} {start_year}-{end_year}")
    
    # Coordonnées des villes
    cities_coords = {
        'paris': {'lat': 48.8566, 'lon': 2.3522, 'country': 'france'},
        'london': {'lat': 51.5074, 'lon': -0.1278, 'country': 'uk'},
        'berlin': {'lat': 52.5200, 'lon': 13.4050, 'country': 'germany'}
    }
    
    coords = cities_coords[city.lower()]
    producer = MockKafkaProducer(topic_queue)
    
    total_months = 0
    success = 0
    
    for year in range(start_year, end_year + 1):
        print(f"PRODUCTEUR: Traitement année {year}")
        
        for month in range(1, 13):
            total_months += 1
            try:
                # Paramètres API
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
                
                print(f"PRODUCTEUR: Requête API {city} {year}/{month:02d}")
                
                # Récupérer les données
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
                
                # Envoyer à Kafka (simulé)
                future = producer.send('weather_history', value=message)
                future.get(timeout=10)
                
                points = len(data.get('hourly', {}).get('time', []))
                print(f"PRODUCTEUR:   Mois {month:02d}: {points} points -> Kafka")
                success += 1
                
                time.sleep(0.2)  # Pause entre requêtes
                
            except Exception as e:
                print(f"PRODUCTEUR:   Erreur mois {month}: {e}")
    
    producer.close()
    print(f"PRODUCTEUR: Terminé - {success}/{total_months} mois traités")
    
    # Signal de fin
    topic_queue.put("__END__")

def consumer_worker(topic_queue):
    """Thread consumer - lit depuis la queue et sauvegarde en HDFS"""
    print("CONSUMER: Démarrage")
    
    consumer = MockKafkaConsumer(topic_queue)
    message_count = 0
    
    for message in consumer.consume():
        data = message.value
        
        # Signal de fin
        if data == "__END__":
            break
            
        try:
            # Sauvegarder en HDFS
            country = data['country']
            city = data['city']
            year = data['year']
            month = data['month']
            
            # Créer le répertoire
            hdfs_path = Path("./hdfs-data") / country / city / "weather_history"
            hdfs_path.mkdir(parents=True, exist_ok=True)
            
            # Nom du fichier
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"weather_{year}_{month:02d}_{timestamp}.json"
            
            # Sauvegarder
            file_path = hdfs_path / filename
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            message_count += 1
            data_points = len(data.get('data', {}).get('time', []))
            
            print(f"CONSUMER: [{message_count}] {city} {year}/{month:02d} -> {file_path.name}")
            print(f"CONSUMER:   {data_points} points, {file_path.stat().st_size} bytes")
            
        except Exception as e:
            print(f"CONSUMER: Erreur traitement message: {e}")
    
    consumer.close()
    print(f"CONSUMER: Terminé - {message_count} messages traités")

def main():
    """Pipeline principal simulé"""
    print("PIPELINE KAFKA 10 ANS - MODE SIMULATION")
    print("=" * 50)
    print("Simulation du flux: Producer -> Kafka -> Consumer -> HDFS")
    print("(Sans serveur Kafka réel - utilise une queue Python)")
    print()
    
    # Queue simulant le topic Kafka
    topic_queue = queue.Queue()
    
    # Démarrer le consumer en arrière-plan
    consumer_thread = threading.Thread(
        target=consumer_worker, 
        args=(topic_queue,),
        daemon=True
    )
    consumer_thread.start()
    
    # Attendre que le consumer soit prêt
    time.sleep(1)
    
    # Démarrer le producteur
    start_time = time.time()
    
    # Pour la démo, utilisons 2014-2024 (10 ans complets)
    producer_worker(topic_queue, 2014, 2024, 'paris')
    
    # Attendre que le consumer termine
    consumer_thread.join(timeout=10)
    
    duration = time.time() - start_time
    
    print(f"\n" + "=" * 50)
    print(f"PIPELINE TERMINÉ")
    print(f"Temps total: {duration:.2f} secondes")
    
    # Vérifier les résultats
    hdfs_path = Path("./hdfs-data/france/paris/weather_history")
    if hdfs_path.exists():
        files = list(hdfs_path.glob("*.json"))
        total_size = sum(f.stat().st_size for f in files)
        total_points = 0
        
        # Compter les points de données
        for file_path in files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    total_points += len(data.get('data', {}).get('time', []))
            except:
                pass
        
        print(f"\nRÉSULTATS:")
        print(f"  Fichiers créés: {len(files)}")
        print(f"  Taille totale: {total_size / 1024:.1f} KB")
        print(f"  Points de données: {total_points:,}")
        print(f"  Moyenne points/fichier: {total_points // len(files) if files else 0}")
        
    print(f"\nPour un vrai pipeline Kafka:")
    print(f"  1. Démarrer: docker-compose up -d")
    print(f"  2. Consumer: python simple_consumer.py &")
    print(f"  3. Producer: python simple_producer.py")

if __name__ == "__main__":
    main()