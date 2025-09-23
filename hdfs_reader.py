#!/usr/bin/env python3
"""
HDFS Logs Reader pour l'exercice 8
Lit et parse tous les fichiers d'alertes météo depuis la structure HDFS organisée
"""

import json
import os
import glob
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

class HDFSLogsReader:
    """Lecteur de logs HDFS pour analyser les alertes météo"""
    
    def __init__(self, hdfs_base_path: str = "./hdfs-data"):
        self.hdfs_base_path = Path(hdfs_base_path)
        self.alerts_data = []
        
    def read_all_alerts(self) -> List[Dict[str, Any]]:
        """Lit tous les fichiers d'alertes de la structure HDFS"""
        print(f"Lecture des logs HDFS depuis {self.hdfs_base_path}")
        
        # Pattern pour trouver tous les fichiers d'alertes
        pattern = str(self.hdfs_base_path / "**" / "*.json")
        alert_files = glob.glob(pattern, recursive=True)
        
        print(f"Trouvé {len(alert_files)} fichiers d'alertes")
        
        for file_path in alert_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    alert_data = json.load(f)
                    
                # Ajouter des métadonnées sur le fichier
                alert_data['file_info'] = {
                    'path': file_path,
                    'filename': os.path.basename(file_path)
                }
                
                self.alerts_data.append(alert_data)
                print(f"Lu {file_path}")
                
            except Exception as e:
                print(f"Erreur lors de la lecture de {file_path}: {e}")
        
        print(f"Total: {len(self.alerts_data)} alertes chargées")
        return self.alerts_data
    
    def get_alerts_summary(self) -> Dict[str, Any]:
        """Génère un résumé des alertes chargées"""
        if not self.alerts_data:
            self.read_all_alerts()
        
        summary = {
            'total_alerts': len(self.alerts_data),
            'countries': set(),
            'cities': set(),
            'alert_types': {},
            'severities': {},
            'timeline': {
                'earliest': None,
                'latest': None
            }
        }
        
        for alert in self.alerts_data:
            # Géographie
            country = alert['location']['country']
            city = alert['location']['city']
            summary['countries'].add(country)
            summary['cities'].add(city)
            
            # Types d'alertes
            for alert_detail in alert['alerts']:
                alert_type = alert_detail['type']
                severity = alert_detail['severity']
                
                summary['alert_types'][alert_type] = summary['alert_types'].get(alert_type, 0) + 1
                summary['severities'][severity] = summary['severities'].get(severity, 0) + 1
            
            # Timeline
            timestamp = alert['timestamps']['original']
            if summary['timeline']['earliest'] is None or timestamp < summary['timeline']['earliest']:
                summary['timeline']['earliest'] = timestamp
            if summary['timeline']['latest'] is None or timestamp > summary['timeline']['latest']:
                summary['timeline']['latest'] = timestamp
        
        # Convertir les sets en listes pour l'affichage
        summary['countries'] = list(summary['countries'])
        summary['cities'] = list(summary['cities'])
        
        return summary
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convertit les données d'alertes en DataFrame pour analyse"""
        if not self.alerts_data:
            self.read_all_alerts()
        
        rows = []
        for alert_file in self.alerts_data:
            location = alert_file['location']
            weather = alert_file['weather_data']
            timestamps = alert_file['timestamps']
            
            for alert_detail in alert_file['alerts']:
                row = {
                    # Géographie
                    'country': location['country'],
                    'city': location['city'],
                    'latitude': location['latitude'],
                    'longitude': location['longitude'],
                    
                    # Météo
                    'temperature': weather['temperature'],
                    'humidity': weather['humidity'],
                    'wind_speed': weather['wind_speed'],
                    
                    # Alerte
                    'alert_type': alert_detail['type'],
                    'severity': alert_detail['severity'],
                    'alert_message': alert_detail['message'],
                    'threshold': alert_detail['threshold'],
                    'value': alert_detail['value'],
                    
                    # Timestamps
                    'original_time': pd.to_datetime(timestamps['original']),
                    'transformed_time': pd.to_datetime(timestamps['transformed']),
                    'saved_time': pd.to_datetime(timestamps['saved_to_hdfs']),
                    
                    # Métadonnées
                    'max_severity': alert_file['alert_summary']['max_severity'],
                    'alert_count': alert_file['alert_summary']['count']
                }
                rows.append(row)
        
        df = pd.DataFrame(rows)
        print(f"DataFrame créé avec {len(df)} alertes")
        return df
    
    def get_geographic_data(self) -> pd.DataFrame:
        """Retourne les données géographiques pour la cartographie"""
        df = self.to_dataframe()
        
        # Agrégation par localisation
        geo_data = df.groupby(['country', 'city', 'latitude', 'longitude']).agg({
            'alert_type': 'count',
            'severity': lambda x: x.value_counts().to_dict(),
            'temperature': 'mean',
            'humidity': 'mean',
            'wind_speed': 'mean'
        }).reset_index()
        
        geo_data.columns = ['country', 'city', 'latitude', 'longitude', 'total_alerts', 
                           'severity_distribution', 'avg_temperature', 'avg_humidity', 'avg_wind_speed']
        
        return geo_data
    
    def display_summary(self):
        """Affiche un résumé formaté des données"""
        summary = self.get_alerts_summary()
        
        print("\n" + "="*60)
        print("RÉSUMÉ DES LOGS HDFS - EXERCICE 8")
        print("="*60)
        print(f"Total alertes: {summary['total_alerts']}")
        print(f"Pays: {', '.join(summary['countries'])}")
        print(f"Villes: {', '.join(summary['cities'])}")
        
        print(f"\nTypes d'alertes:")
        for alert_type, count in summary['alert_types'].items():
            print(f"   • {alert_type}: {count}")
        
        print(f"\nSévérités:")
        for severity, count in summary['severities'].items():
            print(f"   • {severity}: {count}")
        
        print(f"\nTimeline:")
        print(f"   • Première alerte: {summary['timeline']['earliest']}")
        print(f"   • Dernière alerte: {summary['timeline']['latest']}")

if __name__ == "__main__":
    # Test du lecteur HDFS
    reader = HDFSLogsReader()
    
    # Lire toutes les alertes
    alerts = reader.read_all_alerts()
    
    # Afficher le résumé
    reader.display_summary()
    
    # Créer le DataFrame
    df = reader.to_dataframe()
    print(f"\nColonnes disponibles: {list(df.columns)}")
    
    # Données géographiques
    geo_df = reader.get_geographic_data()
    print(f"\nDonnées géographiques: {len(geo_df)} localisations")
    print(geo_df[['country', 'city', 'total_alerts', 'avg_temperature']])