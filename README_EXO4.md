# Exercice 4 : Transformation avec Spark et alertes météo

## Objectif
Transformer les données météo de `weather_stream` avec Apache Spark et générer des alertes dans `weather_transformed`.

## Règles d'alertes

**Vent (wind_alert_level):**
- `level_0` : < 10 m/s 
- `level_1` : 10-20 m/s
- `level_2` : > 20 m/s

**Chaleur (heat_alert_level):**
- `level_0` : < 25°C
- `level_1` : 25-35°C  
- `level_2` : > 35°C

## Démarrage rapide

### 1. Créer le topic
```bash
docker exec kafka kafka-topics --create --topic weather_transformed --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 2. Lancer Spark
```bash
python spark_weather_processor.py
```

### 3. Envoyer données météo
```bash
python producer.py weather_stream
```

### 4. Voir les alertes
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic weather_transformed --from-beginning
```

## Résultats obtenus

### Producteur météo en action
 exo4.png

### Processeur d'alertes en temps réel
exo4_1.png
```

## Validation

- Nice (27°C, 20 m/s) → Alerte chaleur niveau 1, vent niveau 2
- Paris (16.5°C, 17.9 m/s) → Pas d'alerte chaleur, vent niveau 1  
- Lyon (17.3°C, 4.8 m/s) → Aucune alerte

Le système fonctionne parfaitement !

### 2. Lancement du processeur d'alertes
```bash
# Version recommandée (kafka-python)
python weather_alert_processor.py
```

### 3. Test avec producteur météo
```bash
# Dans un autre terminal
python producer.py weather_stream
```

### 4. Vérification des alertes
```bash
# Consumer pour voir les données transformées
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather_transformed \
  --from-beginning
```

## Exemples de transformation

### Cas 1: Conditions normales (Lyon)
- **Input**: 17.3°C, 4.8 m/s
- **Output**: `wind_alert_level: "level_0"`, `heat_alert_level: "level_0"`

### Cas 2: Vent fort (Nice) 
- **Input**: 27.0°C, 20.0 m/s
- **Output**: `wind_alert_level: "level_2"`, `heat_alert_level: "level_1"`

### Cas 3: Vent modéré (Paris)
- **Input**: 16.5°C, 17.9 m/s  
- **Output**: `wind_alert_level: "level_1"`, `heat_alert_level: "level_0"`

## Topics Kafka

| Topic | Description | Format |
|-------|-------------|--------|
| `weather_stream` | Données météo brutes | JSON météo standard |
| `weather_transformed` | Données enrichies avec alertes | JSON + niveaux d'alerte |

## Monitoring

Le processeur affiche en temps réel les transformations :
```
OK Nice: 27.0°C, 20.0 m/s
   -> Alertes: Vent=level_2, Chaleur=level_1
```

## Dépendances

- `kafka-python` : Interface Kafka
- `requests` : API météo
- `pyspark` : Version Spark (optionnelle)

## Notes techniques

### Version simplifiée
Le `weather_alert_processor.py` utilise kafka-python directement, évitant les complexités de configuration Spark/Hadoop sur Windows.

### Version Spark
Le `spark_weather_processor.py` implémente la même logique avec Spark Streaming mais peut nécessiter des configurations Java supplémentaires.

