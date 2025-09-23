#!/usr/bin/env python3
"""
Exercice 7 - Consumer HDFS pour alertes météo
Lit weather_transformed et sauvegarde dans HDFS organisé
Structure: /hdfs-data/{country}/{city}/alerts.json
"""

import json
import os
import sys
from datetime import datetime
from kafka import KafkaConsumer
import logging
from pathlib import Path

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HDFSWeatherConsumer:
    def __init__(self, hdfs_base_path="/hdfs-data"):
        # Consumer pour lire weather_transformed
        self.consumer = KafkaConsumer(
            'weather_transformed',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Lire depuis le début
            enable_auto_commit=True,
            group_id='hdfs_consumer_group_v2',  # Nouveau groupe pour relire
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Chemin de base pour HDFS (simulé localement)
        self.hdfs_base_path = hdfs_base_path
        self.ensure_base_directory()
        
        # Statistiques
        self.stats = {
            'total_messages': 0,
            'messages_with_alerts': 0,
            'files_created': 0,
            'countries': set(),
            'cities': set()
        }

    def ensure_base_directory(self):
        """Assure que le répertoire de base existe"""
        os.makedirs(self.hdfs_base_path, exist_ok=True)
        logger.info(f"Répertoire HDFS configuré: {self.hdfs_base_path}")

    def get_hdfs_path(self, country, city):
        """Génère le chemin HDFS pour un pays/ville"""
        # Nettoie les noms pour éviter les problèmes de chemins
        clean_country = self.clean_name(country)
        clean_city = self.clean_name(city)
        
        return os.path.join(self.hdfs_base_path, clean_country, clean_city)

    def clean_name(self, name):
        """Nettoie un nom pour l'utiliser dans un chemin de fichier"""
        if not name:
            return "unknown"
        # Remplace les caractères problématiques
        cleaned = "".join(c for c in name if c.isalnum() or c in ('-', '_'))
        return cleaned.lower()

    def save_alerts_to_hdfs(self, message_data):
        """Sauvegarde les alertes dans HDFS avec la structure organisée"""
        country = message_data.get('country', 'unknown')
        city = message_data.get('city', 'unknown')
        
        # Ne sauvegarde que s'il y a des alertes
        if not message_data.get('has_alerts', False):
            return False
            
        # Crée le chemin de destination
        hdfs_path = self.get_hdfs_path(country, city)
        os.makedirs(hdfs_path, exist_ok=True)
        
        # Nom du fichier avec timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"alerts_{timestamp}.json"
        file_path = os.path.join(hdfs_path, filename)
        
        # Prépare les données à sauvegarder
        alert_data = {
            'location': {
                'city': city,
                'country': country,
                'latitude': message_data.get('latitude'),
                'longitude': message_data.get('longitude')
            },
            'weather_data': {
                'temperature': message_data.get('temperature'),
                'humidity': message_data.get('humidity'),
                'wind_speed': message_data.get('wind_speed')
            },
            'alerts': message_data.get('alerts', []),
            'alert_summary': {
                'count': message_data.get('alert_count', 0),
                'max_severity': message_data.get('max_severity', 'NONE')
            },
            'timestamps': {
                'original': message_data.get('original_timestamp'),
                'transformed': message_data.get('transformation_timestamp'),
                'saved_to_hdfs': datetime.now().isoformat()
            },
            'hdfs_info': {
                'path': hdfs_path,
                'filename': filename
            }
        }
        
        # Sauvegarde le fichier
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(alert_data, f, indent=2, ensure_ascii=False)
            
            self.stats['files_created'] += 1
            self.stats['countries'].add(country)
            self.stats['cities'].add(f"{city}, {country}")
            
            logger.info(f"Alertes sauvegardées: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde: {e}")
            return False

    def print_statistics(self):
        """Affiche les statistiques"""
        print(f"\nStatistiques HDFS Consumer:")
        print(f"  Messages traités: {self.stats['total_messages']}")
        print(f"  Messages avec alertes: {self.stats['messages_with_alerts']}")
        print(f"  Fichiers créés: {self.stats['files_created']}")
        print(f"  Pays uniques: {len(self.stats['countries'])}")
        print(f"  Villes uniques: {len(self.stats['cities'])}")
        
        if self.stats['countries']:
            print(f"  Pays: {', '.join(sorted(self.stats['countries']))}")

    def run(self):
        """Lance le consumer HDFS"""
        print("Démarrage du Consumer HDFS...")
        print(f"Répertoire HDFS: {self.hdfs_base_path}")
        print("Écoute du topic: weather_transformed")
        print("Structure: /hdfs-data/{country}/{city}/alerts.json")
        print("\nEn attente de messages avec alertes... (Ctrl+C pour arrêter)")
        
        try:
            for message in self.consumer:
                try:
                    data = message.value
                    self.stats['total_messages'] += 1
                    
                    city = data.get('city', 'unknown')
                    country = data.get('country', 'unknown')
                    has_alerts = data.get('has_alerts', False)
                    alert_count = data.get('alert_count', 0)
                    
                    print(f"\nMessage {self.stats['total_messages']}: {city}, {country}")
                    
                    if has_alerts:
                        self.stats['messages_with_alerts'] += 1
                        print(f"  {alert_count} alerte(s) détectée(s)")
                        
                        # Affiche les alertes
                        for alert in data.get('alerts', []):
                            print(f"    - {alert['type']}: {alert['message']}")
                        
                        # Sauvegarde dans HDFS
                        if self.save_alerts_to_hdfs(data):
                            print(f"  Sauvegardé dans HDFS")
                        else:
                            print(f"  Erreur de sauvegarde")
                    else:
                        print(f"  Aucune alerte - non sauvegardé")
                    
                except Exception as e:
                    logger.error(f"Erreur lors du traitement du message: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print(f"\nConsumer HDFS arrêté.")
            self.print_statistics()
        finally:
            self.consumer.close()

    def list_hdfs_structure(self):
        """Affiche la structure HDFS créée"""
        print(f"\nStructure HDFS dans {self.hdfs_base_path}:")
        try:
            for root, dirs, files in os.walk(self.hdfs_base_path):
                level = root.replace(self.hdfs_base_path, '').count(os.sep)
                indent = ' ' * 2 * level
                print(f"{indent}{os.path.basename(root)}/")
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    if file.endswith('.json'):
                        print(f"{subindent}{file}")
        except Exception as e:
            logger.error(f"Erreur lors de l'affichage de la structure: {e}")

if __name__ == "__main__":
    # Permet de spécifier un chemin HDFS personnalisé
    hdfs_path = sys.argv[1] if len(sys.argv) > 1 else "./hdfs-data"
    
    consumer = HDFSWeatherConsumer(hdfs_path)
    
    # Si argument --list, affiche juste la structure
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        consumer.list_hdfs_structure()
    else:
        consumer.run()