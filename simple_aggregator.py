#!/usr/bin/env python3
"""
Agrégateur météo temps réel - Version simplifiée
Exercice 5 - TP Kafka
"""
import json
import time
from datetime import datetime, timedelta
from collections import deque, defaultdict
from kafka import KafkaConsumer
from threading import Thread, Lock
import statistics

class SimpleAggregator:
    """Agrégateur simple pour fenêtres glissantes"""
    
    def __init__(self):
        self.window_1min = deque()
        self.window_5min = deque()
        self.lock = Lock()
        
    def add_message(self, message):
        """Ajoute un message aux fenêtres"""
        now = datetime.now()
        entry = {
            'timestamp': now,
            'data': message
        }
        
        with self.lock:
            self.window_1min.append(entry)
            self.window_5min.append(entry)
            self.cleanup_windows(now)
    
    def cleanup_windows(self, now):
        """Supprime les anciens messages"""
        # Fenêtre 1 minute
        while (self.window_1min and 
               (now - self.window_1min[0]['timestamp']).total_seconds() > 60):
            self.window_1min.popleft()
            
        # Fenêtre 5 minutes
        while (self.window_5min and 
               (now - self.window_5min[0]['timestamp']).total_seconds() > 300):
            self.window_5min.popleft()
    
    def get_stats(self):
        """Calcule les statistiques pour chaque fenêtre"""
        with self.lock:
            stats = {}
            
            for name, window in [('1min', self.window_1min), ('5min', self.window_5min)]:
                if not window:
                    stats[name] = {'count': 0}
                    continue
                
                messages = [entry['data'] for entry in window]
                temperatures = [m.get('temperature', 0) for m in messages]
                winds = [m.get('wind_speed', 0) for m in messages]
                
                # Compter les alertes
                alerts = defaultdict(int)
                for msg in messages:
                    level = msg.get('alert_level', 'niveau_0')
                    alerts[level] += 1
                
                stats[name] = {
                    'count': len(messages),
                    'temp_avg': round(statistics.mean(temperatures), 1) if temperatures else 0,
                    'temp_min': round(min(temperatures), 1) if temperatures else 0,
                    'temp_max': round(max(temperatures), 1) if temperatures else 0,
                    'wind_avg': round(statistics.mean(winds), 1) if winds else 0,
                    'alerts': dict(alerts),
                    'period_start': min(entry['timestamp'] for entry in window).strftime('%H:%M:%S'),
                    'period_end': max(entry['timestamp'] for entry in window).strftime('%H:%M:%S')
                }
            
            return stats
    
    def display_stats(self):
        """Affiche les statistiques"""
        stats = self.get_stats()
        now = datetime.now().strftime('%H:%M:%S')
        
        print(f"\nSTATISTIQUES TEMPS REEL - {now}")
        print("=" * 50)
        
        for window_name, data in stats.items():
            print(f"\nFENETRE {window_name.upper()}")
            print("-" * 30)
            
            if data['count'] == 0:
                print("Aucune donnée")
                continue
                
            print(f"Messages: {data['count']}")
            print(f"Période: {data['period_start']} - {data['period_end']}")
            print(f"Température: moy={data['temp_avg']}°C, min={data['temp_min']}°C, max={data['temp_max']}°C")
            print(f"Vent moyen: {data['wind_avg']} km/h")
            
            if data['alerts']:
                alerts_str = ", ".join([f"{k}={v}" for k, v in data['alerts'].items()])
                print(f"Alertes: {alerts_str}")

def main():
    """Fonction principale"""
    print("AGREGATEUR METEORO EN TEMPS REEL")
    print("Exercice 5 - TP Kafka")
    print("=" * 40)
    
    # Initialisation
    aggregator = SimpleAggregator()
    
    # Configuration Kafka
    consumer = KafkaConsumer(
        'weather_transformed',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    def consume_loop():
        """Boucle de consommation"""
        try:
            for message in consumer:
                aggregator.add_message(message.value)
        except Exception as e:
            print(f"Erreur consumer: {e}")
    
    # Démarrer la consommation en arrière-plan
    consumer_thread = Thread(target=consume_loop)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    print("En attente de messages sur weather_transformed...")
    print("Affichage toutes les 15 secondes (Ctrl+C pour arrêter)")
    
    try:
        cycle = 0
        while True:
            time.sleep(15)
            cycle += 1
            
            # Effacer l'écran
            import os
            os.system('cls' if os.name == 'nt' else 'clear')
            
            print(f"CYCLE #{cycle}")
            aggregator.display_stats()
            
    except KeyboardInterrupt:
        print("\nArrêt du programme")
        print("Exercice 5 terminé")

if __name__ == "__main__":
    main()