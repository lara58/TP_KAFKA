#!/usr/bin/env python3
"""
Exercice 12 : Validation et enrichissement des profils saisonniers
Validation complète, calcul de statistiques avancées et enrichissement des profils climatiques
"""

import json
import os
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import logging
from typing import Dict, List, Tuple, Optional, Any

# Configuration logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeasonalProfileValidator:
    """Validateur et enrichisseur de profils saisonniers"""
    
    def __init__(self, hdfs_base_path: str = "hdfs-data"):
        self.hdfs_base_path = Path(hdfs_base_path)
        self.validation_rules = {
            'temperature': {'min': -50.0, 'max': 60.0},
            'wind_speed': {'min': 0.0, 'max': 60.0},
            'precipitation': {'min': 0.0, 'max': 1000.0}  # mm par jour
        }
        self.required_months = set(range(1, 13))  # 1 à 12
        logger.info("Validateur de profils saisonniers initialisé")
    
    def find_seasonal_profiles(self) -> List[Tuple[str, str, str]]:
        """Trouve tous les profils saisonniers existants"""
        profiles = []
        
        for country_dir in self.hdfs_base_path.iterdir():
            if country_dir.is_dir():
                for city_dir in country_dir.iterdir():
                    if city_dir.is_dir():
                        profile_dir = city_dir / "seasonal_profile"
                        if profile_dir.exists():
                            for profile_file in profile_dir.glob("*.json"):
                                profiles.append((
                                    country_dir.name,
                                    city_dir.name,
                                    str(profile_file)
                                ))
        
        logger.info(f"Profils saisonniers trouvés: {len(profiles)}")
        return profiles
    
    def load_profile(self, profile_path: str) -> Optional[Dict]:
        """Charge un profil saisonnier depuis un fichier JSON"""
        try:
            with open(profile_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erreur chargement profil {profile_path}: {e}")
            return None
    
    def validate_profile_completeness(self, profile: Dict) -> Dict[str, Any]:
        """Valide la complétude du profil saisonnier"""
        validation_result = {
            'is_complete': True,
            'missing_months': [],
            'issues': []
        }
        
        if 'seasonal_profile' not in profile:
            validation_result['is_complete'] = False
            validation_result['issues'].append("Clé 'seasonal_profile' manquante")
            return validation_result
        
        # Vérifier que tous les mois sont présents
        present_months = set()
        for month_key in profile['seasonal_profile'].keys():
            try:
                month_num = int(month_key)
                present_months.add(month_num)
            except ValueError:
                validation_result['issues'].append(f"Clé de mois invalide: {month_key}")
        
        missing_months = self.required_months - present_months
        if missing_months:
            validation_result['is_complete'] = False
            validation_result['missing_months'] = sorted(list(missing_months))
            validation_result['issues'].append(f"Mois manquants: {sorted(list(missing_months))}")
        
        return validation_result
    
    def validate_values_realism(self, profile: Dict) -> Dict[str, Any]:
        """Valide le réalisme des valeurs climatiques"""
        validation_result = {
            'is_realistic': True,
            'unrealistic_values': [],
            'issues': []
        }
        
        if 'seasonal_profile' not in profile:
            return validation_result
        
        for month_key, month_data in profile['seasonal_profile'].items():
            month_issues = []
            
            # Validation température
            if 'temperature' in month_data:
                temp_data = month_data['temperature']
                if 'average' in temp_data:
                    temp_avg = temp_data['average']
                    temp_rules = self.validation_rules['temperature']
                    if not (temp_rules['min'] <= temp_avg <= temp_rules['max']):
                        month_issues.append(
                            f"Température moyenne irréaliste: {temp_avg}°C "
                            f"(limite: {temp_rules['min']}-{temp_rules['max']}°C)"
                        )
            
            # Validation vent
            if 'wind' in month_data:
                wind_data = month_data['wind']
                if 'average_speed' in wind_data:
                    wind_avg = wind_data['average_speed']
                    wind_rules = self.validation_rules['wind_speed']
                    if not (wind_rules['min'] <= wind_avg <= wind_rules['max']):
                        month_issues.append(
                            f"Vitesse vent irréaliste: {wind_avg} m/s "
                            f"(limite: {wind_rules['min']}-{wind_rules['max']} m/s)"
                        )
            
            if month_issues:
                validation_result['is_realistic'] = False
                validation_result['unrealistic_values'].append({
                    'month': month_key,
                    'issues': month_issues
                })
        
        return validation_result
    
    def calculate_advanced_statistics(self, raw_data: List[Dict]) -> Dict[str, Any]:
        """Calcule des statistiques avancées (écart-type, médiane, quantiles)"""
        if not raw_data:
            return {}
        
        # Extraire les valeurs numériques
        temperatures = [d.get('temperature', 0) for d in raw_data if d.get('temperature') is not None]
        wind_speeds = [d.get('wind_speed', 0) for d in raw_data if d.get('wind_speed') is not None]
        precipitations = [d.get('precipitation', 0) for d in raw_data if d.get('precipitation') is not None]
        
        statistics = {}
        
        # Statistiques température
        if temperatures:
            temp_array = np.array(temperatures)
            statistics['temperature'] = {
                'mean': float(np.mean(temp_array)),
                'median': float(np.median(temp_array)),
                'std': float(np.std(temp_array)),
                'min': float(np.min(temp_array)),
                'max': float(np.max(temp_array)),
                'q25': float(np.percentile(temp_array, 25)),
                'q75': float(np.percentile(temp_array, 75)),
                'iqr': float(np.percentile(temp_array, 75) - np.percentile(temp_array, 25))
            }
        
        # Statistiques vent
        if wind_speeds:
            wind_array = np.array(wind_speeds)
            statistics['wind'] = {
                'mean': float(np.mean(wind_array)),
                'median': float(np.median(wind_array)),
                'std': float(np.std(wind_array)),
                'min': float(np.min(wind_array)),
                'max': float(np.max(wind_array)),
                'q25': float(np.percentile(wind_array, 25)),
                'q75': float(np.percentile(wind_array, 75)),
                'iqr': float(np.percentile(wind_array, 75) - np.percentile(wind_array, 25))
            }
        
        # Statistiques précipitations
        if precipitations:
            precip_array = np.array(precipitations)
            statistics['precipitation'] = {
                'mean': float(np.mean(precip_array)),
                'median': float(np.median(precip_array)),
                'std': float(np.std(precip_array)),
                'min': float(np.min(precip_array)),
                'max': float(np.max(precip_array)),
                'q25': float(np.percentile(precip_array, 25)),
                'q75': float(np.percentile(precip_array, 75)),
                'iqr': float(np.percentile(precip_array, 75) - np.percentile(precip_array, 25))
            }
        
        return statistics
    
    def detect_anomalies(self, value: float, stats: Dict[str, float], method: str = 'iqr') -> Dict[str, Any]:
        """Détecte les anomalies basées sur les statistiques"""
        if method == 'iqr':
            # Méthode IQR (Interquartile Range)
            q25, q75 = stats['q25'], stats['q75']
            iqr = stats['iqr']
            lower_bound = q25 - 1.5 * iqr
            upper_bound = q75 + 1.5 * iqr
            
            is_anomaly = value < lower_bound or value > upper_bound
            
            return {
                'is_anomaly': is_anomaly,
                'method': 'iqr',
                'bounds': {'lower': lower_bound, 'upper': upper_bound},
                'severity': 'high' if is_anomaly else 'normal'
            }
        
        elif method == 'zscore':
            # Méthode Z-score
            mean, std = stats['mean'], stats['std']
            if std == 0:
                return {'is_anomaly': False, 'method': 'zscore', 'z_score': 0}
            
            z_score = abs((value - mean) / std)
            is_anomaly = z_score > 2.5  # Seuil de 2.5 sigma
            
            return {
                'is_anomaly': is_anomaly,
                'method': 'zscore',
                'z_score': z_score,
                'severity': 'high' if z_score > 3 else 'medium' if z_score > 2 else 'normal'
            }
        
        return {'is_anomaly': False, 'method': 'unknown'}
    
    def enrich_profile(self, profile: Dict, raw_data_by_month: Dict[int, List[Dict]]) -> Dict[str, Any]:
        """Enrichit un profil avec des statistiques avancées"""
        enriched_profile = profile.copy()
        enriched_profile['enrichment_timestamp'] = datetime.now().isoformat()
        enriched_profile['enrichment_version'] = "1.0"
        
        # Enrichir chaque mois
        for month_num in range(1, 13):
            if str(month_num) in enriched_profile.get('seasonal_profile', {}):
                month_data = enriched_profile['seasonal_profile'][str(month_num)]
                raw_month_data = raw_data_by_month.get(month_num, [])
                
                if raw_month_data:
                    # Calculer statistiques avancées
                    advanced_stats = self.calculate_advanced_statistics(raw_month_data)
                    
                    # Enrichir les données température
                    if 'temperature' in month_data and 'temperature' in advanced_stats:
                        temp_stats = advanced_stats['temperature']
                        month_data['temperature'].update({
                            'median': temp_stats['median'],
                            'std': temp_stats['std'],
                            'min_observed': temp_stats['min'],
                            'max_observed': temp_stats['max'],
                            'q25': temp_stats['q25'],
                            'q75': temp_stats['q75'],
                            'iqr': temp_stats['iqr']
                        })
                        
                        # Détection d'anomalies
                        avg_temp = month_data['temperature']['average']
                        anomaly_detection = self.detect_anomalies(avg_temp, temp_stats, 'iqr')
                        month_data['temperature']['anomaly_detection'] = anomaly_detection
                    
                    # Enrichir les données vent
                    if 'wind' in month_data and 'wind' in advanced_stats:
                        wind_stats = advanced_stats['wind']
                        month_data['wind'].update({
                            'median_speed': wind_stats['median'],
                            'std_speed': wind_stats['std'],
                            'min_observed': wind_stats['min'],
                            'max_observed': wind_stats['max'],
                            'q25': wind_stats['q25'],
                            'q75': wind_stats['q75'],
                            'iqr': wind_stats['iqr']
                        })
                        
                        # Détection d'anomalies
                        avg_wind = month_data['wind']['average_speed']
                        anomaly_detection = self.detect_anomalies(avg_wind, wind_stats, 'iqr')
                        month_data['wind']['anomaly_detection'] = anomaly_detection
                    
                    # Enrichir les données précipitations
                    if 'precipitation' in month_data and 'precipitation' in advanced_stats:
                        precip_stats = advanced_stats['precipitation']
                        month_data['precipitation'].update({
                            'median': precip_stats['median'],
                            'std': precip_stats['std'],
                            'min_observed': precip_stats['min'],
                            'max_observed': precip_stats['max'],
                            'q25': precip_stats['q25'],
                            'q75': precip_stats['q75'],
                            'iqr': precip_stats['iqr']
                        })
        
        return enriched_profile
    
    def load_raw_weather_data(self, country: str, city: str) -> Dict[int, List[Dict]]:
        """Charge les données météo brutes pour calcul des statistiques"""
        raw_data_by_month = defaultdict(list)
        
        weather_dir = self.hdfs_base_path / country / city / "weather_history"
        if not weather_dir.exists():
            logger.warning(f"Répertoire données météo introuvable: {weather_dir}")
            return raw_data_by_month
        
        # Charger tous les fichiers météo
        for weather_file in weather_dir.glob("*.json"):
            try:
                with open(weather_file, 'r', encoding='utf-8') as f:
                    weather_data = json.load(f)
                
                # Structure des données avec time, temperature_2m, etc.
                if 'data' in weather_data and isinstance(weather_data['data'], dict):
                    data = weather_data['data']
                    times = data.get('time', [])
                    temperatures = data.get('temperature_2m', [])
                    wind_speeds = data.get('wind_speed_10m', [])
                    precipitations = data.get('precipitation', [])
                    
                    # Associer chaque timestamp avec ses valeurs
                    for i, time_str in enumerate(times):
                        try:
                            date_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                            month = date_obj.month
                            
                            record = {
                                'date': time_str,
                                'temperature': temperatures[i] if i < len(temperatures) else None,
                                'wind_speed': wind_speeds[i] if i < len(wind_speeds) else None,
                                'precipitation': precipitations[i] if i < len(precipitations) else None
                            }
                            raw_data_by_month[month].append(record)
                        except Exception:
                            continue
                
                # Structure alternative avec weather_data
                elif 'weather_data' in weather_data:
                    for record in weather_data['weather_data']:
                        if 'date' in record:
                            try:
                                date_str = record['date']
                                if 'T' in date_str:
                                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                                else:
                                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                                
                                month = date_obj.month
                                raw_data_by_month[month].append(record)
                            except Exception:
                                continue
                                
            except Exception as e:
                logger.warning(f"Erreur lecture fichier météo {weather_file}: {e}")
                continue
        
        total_records = sum(len(data) for data in raw_data_by_month.values())
        logger.info(f"Données brutes chargées pour {city}, {country}: {total_records} enregistrements")
        return raw_data_by_month
    
    def save_enriched_profile(self, enriched_profile: Dict, country: str, city: str) -> str:
        """Sauvegarde le profil enrichi dans HDFS"""
        # Créer la structure de répertoires
        year = datetime.now().year
        enriched_dir = self.hdfs_base_path / country / city / "seasonal_profile_enriched" / str(year)
        enriched_dir.mkdir(parents=True, exist_ok=True)
        
        # Nom du fichier enrichi
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"profile_{timestamp}.json"
        file_path = enriched_dir / filename
        
        # Sauvegarder
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(enriched_profile, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Profil enrichi sauvegardé: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde profil enrichi: {e}")
            return ""
    
    def validate_and_enrich_profile(self, country: str, city: str, profile_path: str) -> Dict[str, Any]:
        """Processus complet de validation et enrichissement d'un profil"""
        logger.info(f"Validation et enrichissement: {city}, {country}")
        
        # Charger le profil
        profile = self.load_profile(profile_path)
        if not profile:
            return {'success': False, 'error': 'Impossible de charger le profil'}
        
        # Validation de complétude
        completeness = self.validate_profile_completeness(profile)
        
        # Validation du réalisme
        realism = self.validate_values_realism(profile)
        
        # Charger les données brutes pour enrichissement
        raw_data_by_month = self.load_raw_weather_data(country, city)
        
        # Enrichissement du profil
        enriched_profile = self.enrich_profile(profile, raw_data_by_month)
        
        # Ajouter les résultats de validation
        enriched_profile['validation_results'] = {
            'completeness': completeness,
            'realism': realism,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        # Sauvegarder le profil enrichi
        saved_path = self.save_enriched_profile(enriched_profile, country, city)
        
        return {
            'success': True,
            'original_profile': profile_path,
            'enriched_profile': saved_path,
            'validation_results': {
                'is_complete': completeness['is_complete'],
                'is_realistic': realism['is_realistic'],
                'issues': completeness['issues'] + realism.get('issues', [])
            },
            'enrichment_applied': bool(raw_data_by_month)
        }

def main():
    """Fonction principale pour l'exercice 12"""
    print("\nEXERCICE 12 : VALIDATION ET ENRICHISSEMENT")
    print("PROFILS SAISONNIERS AVANCÉS")
    print("=" * 80)
    
    # Initialiser le validateur
    validator = SeasonalProfileValidator()
    
    # Trouver tous les profils saisonniers
    profiles = validator.find_seasonal_profiles()
    
    if not profiles:
        print("[ERREUR] Aucun profil saisonnier trouvé")
        return []
    
    print(f"[DÉCOUVERTE] {len(profiles)} profils saisonniers trouvés")
    
    results = []
    
    # Traiter chaque profil
    for country, city, profile_path in profiles:
        print(f"\n[ANALYSE] {city.title()}, {country.title()}")
        print("-" * 60)
        
        result = validator.validate_and_enrich_profile(country, city, profile_path)
        results.append(result)
        
        if result['success']:
            validation = result['validation_results']
            print(f"[VALIDATION] Complet: {validation['is_complete']}")
            print(f"[VALIDATION] Réaliste: {validation['is_realistic']}")
            
            if validation['issues']:
                print(f"[ATTENTION] Problèmes détectés:")
                for issue in validation['issues']:
                    print(f"  - {issue}")
            
            print(f"[ENRICHISSEMENT] Appliqué: {result['enrichment_applied']}")
            print(f"[SAUVEGARDE] {result['enriched_profile']}")
        else:
            print(f"[ERREUR] {result.get('error', 'Erreur inconnue')}")
    
    # Résumé final
    successful = sum(1 for r in results if r['success'])
    complete_profiles = sum(1 for r in results if r.get('validation_results', {}).get('is_complete', False))
    realistic_profiles = sum(1 for r in results if r.get('validation_results', {}).get('is_realistic', False))
    
    print(f"\n[RÉSUMÉ FINAL]")
    print("=" * 60)
    print(f"[TRAITÉS] {successful}/{len(profiles)} profils traités avec succès")
    print(f"[COMPLETS] {complete_profiles}/{len(profiles)} profils complets (12 mois)")
    print(f"[RÉALISTES] {realistic_profiles}/{len(profiles)} profils avec valeurs réalistes")
    print(f"[ENRICHIS] Profils sauvegardés avec statistiques avancées")
    
    print("\n[SUCCÈS] Exercice 12 - Validation et enrichissement terminé!")
    return results

if __name__ == "__main__":
    results = main()