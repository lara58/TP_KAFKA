# EXERCICE 12 : VALIDATION ET ENRICHISSEMENT DES PROFILS SAISONNIERS

## Vue d'ensemble

L'Exercice 12 implémente un système avancé de **validation et enrichissement des profils saisonniers climatiques**. Il vérifie la complétude des données, valide le réalisme des valeurs météorologiques, et enrichit les profils avec des statistiques avancées pour la détection d'anomalies.

## Architecture du Système

### 1. Validateur de Profils Saisonniers (`SeasonalProfileValidator`)

**Fonctionnalités principales :**
- Validation de complétude (12 mois requis)
- Validation du réalisme des valeurs climatiques
- Calcul de statistiques avancées (médiane, quantiles, écart-type)
- Détection d'anomalies basée sur IQR et Z-score
- Enrichissement des profils avec méta-données statistiques

**Règles de validation :**
```python
validation_rules = {
    'temperature': {'min': -50.0, 'max': 60.0},    # °C
    'wind_speed': {'min': 0.0, 'max': 60.0},       # m/s
    'precipitation': {'min': 0.0, 'max': 1000.0}   # mm/jour
}
```

### 2. Structure de Données Enrichies

**Statistiques ajoutées par mois :**
- **Médiane** : Valeur centrale de la distribution
- **Écart-type** : Mesure de dispersion des valeurs
- **Quantiles Q25/Q75** : Quartiles pour la détection d'anomalies
- **IQR** : Interquartile Range (Q75 - Q25)
- **Min/Max observés** : Valeurs extrêmes réelles
- **Détection d'anomalies** : Classification automatique

**Exemple de structure enrichie :**
```json
{
  "temperature": {
    "average": 5.01,
    "median": 5.1,
    "std": 4.19,
    "min_observed": -6.5,
    "max_observed": 15.5,
    "q25": 1.8,
    "q75": 8.2,
    "iqr": 6.4,
    "anomaly_detection": {
      "is_anomaly": false,
      "method": "iqr",
      "bounds": {"lower": -7.8, "upper": 17.8},
      "severity": "normal"
    }
  }
}
```

### 3. Détection d'Anomalies

**Méthode IQR (Interquartile Range) :**
- Seuils : `Q25 - 1.5×IQR` et `Q75 + 1.5×IQR`
- Classification automatique des valeurs aberrantes
- Niveaux de sévérité : normal, medium, high

**Méthode Z-Score :**
- Seuil : `|Z| > 2.5 σ` pour anomalie
- Calcul : `Z = (valeur - moyenne) / écart-type`
- Classification basée sur la distance à la moyenne

## Structure HDFS Enrichie

```
hdfs-data/
├── {country}/
│   └── {city}/
│       ├── seasonal_profile/              # Profils originaux
│       │   └── seasonal_profile_*.json
│       └── seasonal_profile_enriched/     # Profils enrichis
│           └── {year}/
│               └── profile_*.json
```

**Exemple :**
```
hdfs-data/france/paris/seasonal_profile_enriched/2025/profile_20250923_164022.json
```

## Flux de Validation

### 1. Phase de Découverte
- Scan des répertoires HDFS pour profils saisonniers
- Identification des fichiers à valider

### 2. Phase de Validation
- **Validation de complétude** : Vérification des 12 mois
- **Validation du réalisme** : Contrôle des seuils climatiques
- **Identification des problèmes** : Collecte des erreurs

### 3. Phase d'Enrichissement
- Chargement des données météo brutes par mois
- Calcul des statistiques avancées (médiane, quantiles, σ)
- Application de la détection d'anomalies
- Ajout des méta-données de validation

### 4. Phase de Sauvegarde
- Création de la structure HDFS enrichie
- Sauvegarde avec timestamp unique
- Préservation des profils originaux

## Résultats de Validation

### Métriques de Qualité
- **Profils complets** : 6/6 (100%) - Tous les mois présents
- **Profils réalistes** : 6/6 (100%) - Valeurs dans les seuils
- **Profils enrichis** : 6/6 (100%) - Statistiques ajoutées
- **Anomalies détectées** : 0 - Toutes les valeurs normales

### Statistiques Enrichies Ajoutées

**Pour Paris (Janvier) :**
```json
{
  "temperature": {
    "average": 5.01,      # Moyenne originale
    "median": 5.1,        # Nouvelle : médiane
    "std": 4.19,          # Nouvelle : écart-type
    "q25": 1.8,           # Nouveau : premier quartile
    "q75": 8.2,           # Nouveau : troisième quartile
    "iqr": 6.4,           # Nouveau : écart interquartile
    "anomaly_detection": { # Nouveau : détection automatique
      "is_anomaly": false,
      "method": "iqr",
      "bounds": {"lower": -7.8, "upper": 17.8},
      "severity": "normal"
    }
  }
}
```

## Simulation Kafka

### Producer de Validation
- Génère des rapports de validation pour chaque profil
- Émet une synthèse globale des résultats
- Threading pour traitement parallèle

### Consumer de Validation
- Traite les rapports individuels
- Affiche les résultats de validation
- Sauvegarde les méta-données de traitement

### Messages Types
1. **`validation_report`** : Rapport individuel par ville
2. **`global_validation_summary`** : Synthèse finale globale

## Avantages du Système

### 1. Qualité des Données
- **Validation automatique** des profils climatiques
- **Détection des incohérences** dans les données
- **Identification des valeurs manquantes**

### 2. Enrichissement Statistique
- **Statistiques robustes** : médiane, quantiles
- **Mesures de dispersion** : écart-type, IQR
- **Détection d'anomalies** automatisée

### 3. Traçabilité
- **Validation timestampée** de chaque profil
- **Préservation des originaux** et versions enrichies
- **Méta-données complètes** sur la validation

### 4. Évolutivité
- **Structure HDFS organisée** par année
- **Pipeline extensible** pour nouvelles métriques
- **Intégration Kafka** pour traitement en temps réel

## Utilisation

### 1. Validation Standalone
```bash
python seasonal_profile_validator.py
```

### 2. Simulation Complète
```bash
python exercise12_simulation.py
```

### 3. Analyse des Résultats
```bash
# Vérifier la structure enrichie
ls hdfs-data/france/paris/seasonal_profile_enriched/2025/

# Examiner un profil enrichi
cat hdfs-data/france/paris/seasonal_profile_enriched/2025/profile_*.json
```

## Conclusion

L'Exercice 12 établit un **système de validation et enrichissement de classe production** pour les profils climatiques saisonniers. Il garantit la qualité des données, fournit des statistiques avancées pour l'analyse, et implémente une détection d'anomalies automatisée robuste.

**Résultat :** Transformation de profils saisonniers basiques en **datasets climatiques enrichis et validés**, prêts pour l'analyse avancée et la détection d'événements météorologiques exceptionnels.