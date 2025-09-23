#!/usr/bin/env python3
"""
Exercice 12 : Simulation complète validation et enrichissement
Test complet de la validation et enrichissement des profils saisonniers avec Kafka
"""

import json
import time
from pathlib import Path
from datetime import datetime
from queue import Queue
from threading import Thread
from seasonal_profile_validator import SeasonalProfileValidator
import logging

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockKafkaMessage:
    """Message Kafka simulé pour les profils enrichis"""
    def __init__(self, value: dict, offset: int = 0):
        self.value = json.dumps(value, ensure_ascii=False)
        self.offset = offset

class ValidationProducer:
    """Producteur simulé pour les profils de validation"""
    
    def __init__(self, message_queue: Queue):
        self.message_queue = message_queue
        self.validator = SeasonalProfileValidator()
        
    def produce_validation_reports(self):
        """Produit les rapports de validation pour tous les profils"""
        logger.info("Démarrage de la production des rapports de validation")
        
        # Trouver tous les profils saisonniers
        profiles = self.validator.find_seasonal_profiles()
        
        if not profiles:
            logger.warning("Aucun profil saisonnier trouvé")
            return
        
        validation_reports = []
        
        for country, city, profile_path in profiles:
            print(f"\n[VALIDATION] {city.title()}, {country.title()}")
            
            # Valider et enrichir le profil
            result = self.validator.validate_and_enrich_profile(country, city, profile_path)
            
            if result['success']:
                validation_report = {
                    'message_type': 'validation_report',
                    'timestamp': datetime.now().isoformat(),
                    'city': city,
                    'country': country,
                    'original_profile': result['original_profile'],
                    'enriched_profile': result['enriched_profile'],
                    'validation_summary': {
                        'is_complete': result['validation_results']['is_complete'],
                        'is_realistic': result['validation_results']['is_realistic'],
                        'issues_count': len(result['validation_results']['issues']),
                        'issues': result['validation_results']['issues']
                    },
                    'enrichment_applied': result['enrichment_applied']
                }
                
                # Afficher résultats
                print(f"   [COMPLET] {validation_report['validation_summary']['is_complete']}")
                print(f"   [RÉALISTE] {validation_report['validation_summary']['is_realistic']}")
                print(f"   [ENRICHI] {validation_report['enrichment_applied']}")
                
                if validation_report['validation_summary']['issues']:
                    print(f"   [PROBLÈMES] {validation_report['validation_summary']['issues_count']} détectés")
                    for issue in validation_report['validation_summary']['issues']:
                        print(f"     - {issue}")
                
                validation_reports.append(validation_report)
                
                # Ajouter à la queue
                message = MockKafkaMessage(validation_report, len(validation_reports))
                self.message_queue.put(message)
                logger.info(f"Rapport de validation émis pour {city}, {country}")
        
        # Ajouter rapport de synthèse globale
        global_summary = {
            'message_type': 'global_validation_summary',
            'timestamp': datetime.now().isoformat(),
            'total_profiles': len(validation_reports),
            'complete_profiles': sum(1 for r in validation_reports if r['validation_summary']['is_complete']),
            'realistic_profiles': sum(1 for r in validation_reports if r['validation_summary']['is_realistic']),
            'enriched_profiles': sum(1 for r in validation_reports if r['enrichment_applied']),
            'total_issues': sum(r['validation_summary']['issues_count'] for r in validation_reports),
            'validation_summary': validation_reports
        }
        
        message = MockKafkaMessage(global_summary, len(validation_reports) + 1)
        self.message_queue.put(message)
        logger.info("Synthèse globale de validation émise")
        
        print(f"\n[PRODUCTION] {len(validation_reports)} rapports de validation émis")
        return validation_reports

class ValidationConsumer:
    """Consumer simulé pour traiter les rapports de validation"""
    
    def __init__(self, message_queue: Queue):
        self.message_queue = message_queue
        self.processed_reports = 0
        
    def consume_validation_reports(self):
        """Consume et traite les rapports de validation"""
        logger.info("Démarrage de la consommation des rapports")
        
        while True:
            try:
                # Récupérer message avec timeout
                message = self.message_queue.get(timeout=5)
                
                # Parser le message
                data = json.loads(message.value)
                message_type = data.get('message_type')
                
                if message_type == 'validation_report':
                    self.process_validation_report(data, message.offset)
                    
                elif message_type == 'global_validation_summary':
                    self.process_global_summary(data, message.offset)
                    break  # Fin des messages
                
                self.processed_reports += 1
                
            except Exception as e:
                logger.info("Fin de la consommation des messages")
                break
        
        print(f"\n[CONSOMMATION] {self.processed_reports} messages traités")
    
    def process_validation_report(self, report: dict, offset: int):
        """Traite un rapport de validation individuel"""
        city = report['city'].title()
        country = report['country'].title()
        
        print(f"\n[TRAITEMENT] Rapport pour {city}, {country}")
        
        validation = report['validation_summary']
        
        if validation['is_complete'] and validation['is_realistic']:
            print(f"   [SUCCÈS] Profil valide et enrichi")
            print(f"   [ENRICHI] {report['enriched_profile']}")
        else:
            print(f"   [ATTENTION] Problèmes détectés:")
            if not validation['is_complete']:
                print(f"     - Profil incomplet")
            if not validation['is_realistic']:
                print(f"     - Valeurs non réalistes")
            for issue in validation['issues']:
                print(f"     - {issue}")
        
        print(f"   [TRAITÉ] (Offset: {offset})")
    
    def process_global_summary(self, summary: dict, offset: int):
        """Traite la synthèse globale de validation"""
        print(f"\n[SYNTHÈSE GLOBALE VALIDATION]")
        print("=" * 70)
        print(f"   [PROFILS] {summary['total_profiles']} profils analysés")
        print(f"   [COMPLETS] {summary['complete_profiles']}/{summary['total_profiles']} profils complets")
        print(f"   [RÉALISTES] {summary['realistic_profiles']}/{summary['total_profiles']} profils réalistes")
        print(f"   [ENRICHIS] {summary['enriched_profiles']}/{summary['total_profiles']} profils enrichis")
        print(f"   [PROBLÈMES] {summary['total_issues']} problèmes totaux détectés")
        print(f"   [TRAITÉ] (Offset: {offset})")

def producer_thread(queue: Queue):
    """Thread pour la production des rapports de validation"""
    print("[PROCESS] Thread Producteur : Validation et enrichissement")
    
    producer = ValidationProducer(queue)
    validation_reports = producer.produce_validation_reports()
    
    print(f"\n[SUCCÈS] Producteur terminé : {len(validation_reports)} rapports émis")

def consumer_thread(queue: Queue):
    """Thread pour la consommation des rapports"""
    print("[PROCESS] Thread Consumer : Traitement des rapports")
    
    consumer = ValidationConsumer(queue)
    consumer.consume_validation_reports()
    
    print(f"\n[SUCCÈS] Consumer terminé : {consumer.processed_reports} rapports traités")

def main():
    """Fonction principale de la simulation Exercice 12"""
    print("\nEXERCICE 12 : SIMULATION COMPLÈTE")
    print("VALIDATION ET ENRICHISSEMENT DES PROFILS SAISONNIERS")
    print("=" * 80)
    print("Mode simulation - Validation avancée avec statistiques enrichies")
    
    try:
        print("\n[1] Initialisation du validateur...")
        validator = SeasonalProfileValidator()
        
        # Vérifier qu'il y a des profils à valider
        profiles = validator.find_seasonal_profiles()
        if not profiles:
            print("[ERREUR] Aucun profil saisonnier trouvé")
            return
        
        print(f"[DÉCOUVERTE] {len(profiles)} profils trouvés : {[f'{c}/{ci}' for c, ci, _ in profiles]}")
        
        print("\n[2] Initialisation des composants simulés...")
        
        # Queue pour les messages
        message_queue = Queue()
        
        print("\n[3] Lancement de la validation et enrichissement...")
        
        # Créer et lancer les threads
        producer = Thread(target=producer_thread, args=(message_queue,))
        consumer = Thread(target=consumer_thread, args=(message_queue,))
        
        # Démarrer les threads
        producer.start()
        time.sleep(1)  # Délai pour que le producteur commence
        consumer.start()
        
        # Attendre la fin des threads
        producer.join()
        consumer.join()
        
        # Vérifier la structure finale
        print("\n[4] Vérification de la structure HDFS...")
        
        enriched_count = 0
        for country_dir in Path("hdfs-data").iterdir():
            if country_dir.is_dir():
                for city_dir in country_dir.iterdir():
                    if city_dir.is_dir():
                        enriched_dir = city_dir / "seasonal_profile_enriched"
                        if enriched_dir.exists():
                            for year_dir in enriched_dir.iterdir():
                                if year_dir.is_dir():
                                    enriched_files = list(year_dir.glob("*.json"))
                                    enriched_count += len(enriched_files)
                                    print(f"   hdfs-data\\{country_dir.name}\\{city_dir.name}\\seasonal_profile_enriched\\{year_dir.name}: {len(enriched_files)} profil(s) enrichi(s)")
        
        print(f"\n[RÉSULTATS DE LA SIMULATION]")
        print("=" * 60)
        print(f"[PROFILS] {len(profiles)} profils analysés")
        print(f"[ENRICHIS] {enriched_count} profils enrichis sauvegardés")
        print(f"[STRUCTURE] Structure HDFS enrichie créée")
        print(f"[STATISTIQUES] Médiane, quantiles, écart-type ajoutés")
        print(f"[ANOMALIES] Détection automatique implémentée")
        
        print("\n[SUCCÈS] Exercice 12 - Validation et enrichissement terminé avec succès !")
        
    except Exception as e:
        logger.error(f"Erreur dans la simulation: {e}")
        print(f"[ERREUR] {e}")

if __name__ == "__main__":
    main()