#!/usr/bin/env python3
"""
Exercice 4 : Consumer Groups et Partitions
Démonstration des consumer groups avec plusieurs consumers qui se partagent un topic partitionné
"""

import json
import sys
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import threading
import signal

class MultiConsumer:
    def __init__(self, topic_name, group_id, consumer_id):
        self.topic_name = topic_name
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.running = True
        self.message_count = 0
        
    def start_consumer(self):
        """Démarre un consumer dans le groupe spécifié"""
        print(f"[{self.consumer_id}] Démarrage du consumer dans le groupe '{self.group_id}'")
        
        try:
            consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=['localhost:9092'],
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Lire depuis le début
                enable_auto_commit=True,
                consumer_timeout_ms=10000  # Timeout après 10 secondes sans message
            )
            
            print(f"[{self.consumer_id}] Connecté au topic '{self.topic_name}' avec le groupe '{self.group_id}'")
            print(f"[{self.consumer_id}] En attente de messages...")
            
            for message in consumer:
                if not self.running:
                    break
                    
                self.message_count += 1
                data = message.value
                
                print(f"\n[{self.consumer_id}] Message reçu #{self.message_count}:")
                print(f"  Partition: {message.partition}")
                print(f"  Offset: {message.offset}")
                print(f"  Ville: {data.get('city', 'N/A')}")
                print(f"  Température: {data.get('temperature', 'N/A')}°C")
                print(f"  ID Message: {data.get('message_id', 'N/A')}")
                print(f"  Timestamp: {data.get('timestamp', 'N/A')}")
                print("-" * 60)
                
                # Simuler un traitement
                time.sleep(1)
                
        except Exception as e:
            print(f"[{self.consumer_id}] Erreur: {e}")
        finally:
            print(f"[{self.consumer_id}] Consumer fermé - {self.message_count} messages traités")
    
    def stop(self):
        """Arrête le consumer"""
        self.running = False

def signal_handler(sig, frame):
    """Gestionnaire pour arrêter proprement avec Ctrl+C"""
    print("\nArrêt des consumers...")
    global consumers
    for consumer in consumers:
        consumer.stop()

def main():
    if len(sys.argv) < 3:
        print("Usage: python consumer_group.py <topic_name> <group_id> [nb_consumers]")
        print("Exemple: python consumer_group.py weather_stream weather_group 3")
        sys.exit(1)
    
    topic_name = sys.argv[1]
    group_id = sys.argv[2]
    nb_consumers = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    
    print(f"Démarrage de {nb_consumers} consumers pour le topic '{topic_name}'")
    print(f"Groupe de consumers: '{group_id}'")
    print("Chaque consumer va traiter des partitions différentes")
    print("Appuyez sur Ctrl+C pour arrêter tous les consumers\n")
    
    global consumers
    consumers = []
    threads = []
    
    # Gestionnaire de signal pour arrêt propre
    signal.signal(signal.SIGINT, signal_handler)
    
    # Créer et démarrer plusieurs consumers
    for i in range(nb_consumers):
        consumer_id = f"Consumer-{i+1}"
        consumer = MultiConsumer(topic_name, group_id, consumer_id)
        consumers.append(consumer)
        
        # Chaque consumer dans son propre thread
        thread = threading.Thread(target=consumer.start_consumer)
        thread.daemon = True
        threads.append(thread)
        thread.start()
    
    try:
        # Attendre que tous les threads se terminent
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
    
    print("\nTous les consumers sont arrêtés.")
    
    # Afficher le résumé
    total_messages = sum(consumer.message_count for consumer in consumers)
    print(f"\nRésumé:")
    for consumer in consumers:
        print(f"  {consumer.consumer_id}: {consumer.message_count} messages")
    print(f"  Total: {total_messages} messages traités")

if __name__ == "__main__":
    main()