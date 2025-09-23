#!/usr/bin/env python3
"""
Script pour examiner les données HDFS sauvegardées
"""

import json
from pathlib import Path
from datetime import datetime

def analyze_hdfs_data():
    """Analyse les données stockées dans HDFS"""
    print("ANALYSE DES DONNÉES HDFS SAUVEGARDÉES")
    print("=" * 50)
    
    hdfs_path = Path("./hdfs-data/france/paris/weather_history")
    
    if not hdfs_path.exists():
        print("Aucune donnée HDFS trouvée")
        return
    
    # Lister tous les fichiers JSON
    json_files = list(hdfs_path.glob("*.json"))
    
    print(f"Nombre de fichiers: {len(json_files)}")
    
    # Analyser quelques fichiers
    total_points = 0
    total_size = 0
    years_data = {}
    
    for file_path in json_files:
        try:
            # Taille du fichier
            file_size = file_path.stat().st_size
            total_size += file_size
            
            # Contenu du fichier
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extraire les informations
            year = data.get('year')
            month = data.get('month')
            city = data.get('city')
            country = data.get('country')
            
            # Compter les points de données
            weather_data = data.get('data', {})
            time_points = weather_data.get('time', [])
            data_points = len(time_points)
            total_points += data_points
            
            # Grouper par année
            if year not in years_data:
                years_data[year] = {'months': 0, 'points': 0, 'size': 0}
            
            years_data[year]['months'] += 1
            years_data[year]['points'] += data_points
            years_data[year]['size'] += file_size
            
        except Exception as e:
            print(f"Erreur fichier {file_path.name}: {e}")
    
    # Afficher le résumé
    print(f"\nRÉSUMÉ GLOBAL:")
    print(f"  Total fichiers: {len(json_files)}")
    print(f"  Taille totale: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
    print(f"  Points de données: {total_points:,}")
    print(f"  Moyenne points/fichier: {total_points//len(json_files) if json_files else 0}")
    
    # Afficher par année
    print(f"\nDÉTAIL PAR ANNÉE:")
    for year in sorted(years_data.keys()):
        year_info = years_data[year]
        print(f"  {year}: {year_info['months']} mois, {year_info['points']:,} points, {year_info['size']/1024:.0f} KB")
    
    # Examiner un fichier en détail
    if json_files:
        print(f"\nEXAMEN DÉTAILLÉ - {json_files[0].name}:")
        try:
            with open(json_files[0], 'r', encoding='utf-8') as f:
                sample_data = json.load(f)
            
            print(f"  Ville: {sample_data.get('city')}")
            print(f"  Pays: {sample_data.get('country')}")
            print(f"  Année/Mois: {sample_data.get('year')}/{sample_data.get('month'):02d}")
            print(f"  Timestamp: {sample_data.get('timestamp')}")
            
            weather_data = sample_data.get('data', {})
            print(f"  Variables météo disponibles:")
            for var_name, var_data in weather_data.items():
                if isinstance(var_data, list):
                    print(f"    - {var_name}: {len(var_data)} valeurs")
                    if var_name == 'temperature_2m' and var_data:
                        valid_temps = [t for t in var_data if t is not None]
                        if valid_temps:
                            print(f"      Temperature: {min(valid_temps):.1f}°C à {max(valid_temps):.1f}°C")
            
        except Exception as e:
            print(f"Erreur examen détaillé: {e}")
    
    print(f"\nSTRUCTURE HDFS:")
    print(f"  {hdfs_path}")
    print(f"  ├── {json_files[0].name}")
    print(f"  ├── {json_files[1].name}")
    print(f"  ├── ...")
    print(f"  └── {json_files[-1].name}")

if __name__ == "__main__":
    analyze_hdfs_data()