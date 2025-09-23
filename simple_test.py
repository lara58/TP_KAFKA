#!/usr/bin/env python3
"""
Script de test rapide du pipeline complet
"""

import subprocess
import time
import sys

def test_pipeline():
    """Test du pipeline simplifié"""
    print("TEST PIPELINE MÉTÉO HISTORIQUE")
    print("=" * 40)
    
    # 1. Test API
    print("\n1. Test API...")
    result = subprocess.run([sys.executable, "simple_api_test.py"])
    if result.returncode != 0:
        print("Test API échoué")
        return False
    
    # 2. Vérifier Kafka (optionnel)
    print("\n2. Pipeline Kafka...")
    print("Pour tester avec Kafka:")
    print("   1. Démarrer: docker-compose up -d")
    print("   2. Consumer: python simple_consumer.py &")
    print("   3. Producer: python simple_producer.py --city paris --start-year 2023 --end-year 2024")
    
    # 3. Voir structure HDFS
    print("\n3. Structure HDFS actuelle:")
    try:
        from pathlib import Path
        hdfs_path = Path("./hdfs-data")
        if hdfs_path.exists():
            total_files = 0
            for country_dir in hdfs_path.iterdir():
                if country_dir.is_dir():
                    print(f"Pays {country_dir.name}:")
                    for city_dir in country_dir.iterdir():
                        if city_dir.is_dir():
                            for data_dir in city_dir.iterdir():
                                if data_dir.is_dir():
                                    files = list(data_dir.glob("*.json"))
                                    if files:
                                        total_files += len(files)
                                        print(f"  {city_dir.name}/{data_dir.name}: {len(files)} fichiers")
            print(f"Total: {total_files} fichiers")
        else:
            print("Aucune donnée HDFS")
    except Exception as e:
        print(f"Erreur lecture HDFS: {e}")
    
    print("\nTest terminé!")
    return True

if __name__ == "__main__":
    test_pipeline()