# TP KAFKA - Traitement de Données Météorologiques en Temps Réel

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## 🌦️ Description du Projet

Projet de traitement de données météorologiques en temps réel utilisant Apache Kafka, Python et Spark. Ce TP implémente une architecture de streaming complète pour l'analyse climatologique avec [...]

## 📋 Table des Matières

- [Architecture Kafka](#-architecture-kafka)
- [Exercices Implémentés](#-exercices-implémentés)
- [Installation et Configuration](#-installation-et-configuration)
- [Utilisation](#-utilisation)
- [Structure du Projet](#-structure-du-projet)
- [Résultats et Démonstrations](#-résultats-et-démonstrations)

## 🏗️ Architecture Kafka

### Pourquoi Apache Kafka ?

Apache Kafka résout le problème de complexité dans la gestion des flux de données entre systèmes multiples :

![Capture d'écran 2025-09-23 152219.png](Capture%20d%27%C3%A9cran%202025-09-23%20152219.png)

**Sans Kafka** : Connexions point-à-point complexes entre tous les systèmes

### Découplage des Flux de Données

![Capture d'écran 2025-09-23 152245.png](Capture%20d%27%C3%A9cran%202025-09-23%20152245.png)

**Avec Kafka** : Centralisation des flux de données via un seul point d'entrée

### Architecture Producer-Broker-Consumer

![Capture d'écran 2025-09-23 152357.png](Capture%20d%27%C3%A9cran%202025-09-23%20152357.png)

**Distribution automatique** : Les producers envoient les données vers les brokers qui les répartissent automatiquement

![Capture d'écran 2025-09-23 152422.png](Capture%20d%27%C3%A9cran%202025-09-23%20152422.png)

**Lecture ordonnée** : Les consumers lisent les données dans l'ordre depuis les partitions

### Partitionnement et Scalabilité

![Capture d'écran 2025-09-23 152500.png](Capture%20d%27%C3%A9cran%202025-09-23%20152500.png)

**Partitions multiples** : Chaque topic est divisé en partitions pour une meilleure performance

## 🎯 Exercices Implémentés

### Exercice 10 : Détection des Records Climatiques
- **Objectif** : Détection des valeurs extrêmes (température, vent, précipitations).
- **Technologie** : Analyse directe sans Spark (résolution des incompatibilités).
- **Données** : 99 600 mesures sur 11 années (2014-2024).
- **Résultats** : 
  - Record de chaleur : 40.9°C (Paris, 25/07/2019)
  - Record de froid : -9.9°C (Paris, 08/02/2018)
  - Record de vent : 52.0 m/s (Paris, 12/01/2017)

### Exercice 11 : Climatologie Urbaine - Profils Saisonniers
- **Objectif** : Analyse des patterns climatiques mensuels.
- **Pipeline** : Producer/Consumer Kafka simulé avec threading.
- **Fonctionnalités** :
  - Groupement par mois (12 profils)
  - Classification d'alertes (niveau 1 et 2)
  - Calcul de moyennes, min/max mensuel
- **Sauvegarde** : `/hdfs-data/{country}/{city}/seasonal_profile/`

### Exercice 12 : Validation et Enrichissement des Profils
- **Objectif** : Validation complète et enrichissement statistique.
- **Validations** :
  - Complétude (12 mois obligatoires)
  - Réalisme (température -50°C à +60°C, vent 0-60 m/s)
- **Enrichissements** :
  - Écart-type pour température et vent
  - Médiane et quantiles (Q25, Q75)
  - Détection d'anomalies (méthode IQR)
- **Sauvegarde** : `/hdfs-data/{country}/{city}/seasonal_profile_enriched/{year}/`

## 🚀 Installation et Configuration

### Prérequis
```bash
# Python 3.8+
# Docker et Docker Compose
# Git
```

### Installation
```bash
# Cloner le repository
git clone https://github.com/lara58/TP_KAFKA.git
cd TP_KAFKA

# Créer l'environnement virtuel
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Installer les dépendances
pip install requests numpy
```

### Configuration Docker (optionnel)
```bash
# Lancer Kafka et HDFS
docker-compose up -d
```

## 🎮 Utilisation

### Exercice 10 - Records Climatiques
```bash
# Analyseur direct
python simple_records_analyzer.py

# Simulation complète
python simple_exercise10_simulation.py
```

### Exercice 11 - Profils Saisonniers
```bash
# Analyseur climatologique
python urban_climatology_analyzer.py

# Simulation avec threading (recommandé)
python exercise11_simulation.py
```

### Exercice 12 - Validation et Enrichissement
```bash
# Validateur seul
python seasonal_profile_validator.py

# Simulation complète (recommandé)
python exercise12_simulation.py
```

## 📁 Structure du Projet

```
TP_KAFKA/
├── README.md
├── docker-compose.yml
├── hadoop.env
├── simple_records_analyzer.py          # Analyseur de records
├── simple_exercise10_simulation.py     # Simulation complète
├── urban_climatology_analyzer.py       # Analyseur saisonnier
├── exercise11_simulation.py            # Simulation avec threading
├── seasonal_profile_validator.py       # Validateur de profils
├── exercise12_simulation.py            # Simulation validation
├── EXERCICE12_DOCUMENTATION.md         # Documentation complète
├── hdfs-data/
│   ├── france/paris/
│   │   ├── weather_history/             # Données météo brutes
│   │   ├── seasonal_profile/            # Profils saisonniers
│   │   ├── seasonal_profile_enriched/   # Profils enrichis validés
│   │   └── weather_records/             # Records climatiques
│   └── global_summaries/                # Synthèses globales
```

## 📊 Résultats et Démonstrations

### Données Traitées
- **Volume** : 99 600 mesures météorologiques
- **Période** : 11 années (
