# Exercice 8 : Visualisation des Logs Météo

## Principe

Lecture et visualisation des alertes météo stockées dans HDFS depuis l'exercice 7.

**Pipeline :** HDFS Logs → Lecteur → Agrégation → Visualisations Interactives

## Utilisation

```bash
# Générer toutes les visualisations
python weather_visualizer.py

# Analyser uniquement les données HDFS  
python hdfs_reader.py
```

## Données analysées

- **4 alertes** réparties sur 4 pays (Russia, Singapore, UAE, USA)
- **Types** : TEMPERATURE_HIGH/LOW, HUMIDITY_HIGH, WIND_HIGH
- **Sévérités** : 3 HIGH, 1 MEDIUM
- **Range température** : -25.3°C à 45.5°C

## Visualisations générées

### Fichiers créés dans `visualizations/`
- `alerts_by_country.html` - Distribution par pays
- `severity_distribution.html` - Répartition des sévérités
- `alert_types_heatmap.html` - Heatmap types/pays
- `world_map.html` - Carte mondiale interactive
- `weather_dashboard.html` - Dashboard complet
- `summary_table.html` - Table récapitulative

### Métriques visuelles
- Cartes géographiques interactives
- Graphiques de distribution et tendances
- Corrélations température/vent/humidité
- Timeline chronologique des alertes

## Structure

```
├── hdfs_reader.py          # Lecture des logs HDFS
├── weather_visualizer.py   # Génération des visualisations
└── visualizations/         # Graphiques générés
```

L'exercice 8 démontre un système complet d'analytics météorologique avec dashboards interactifs pour l'aide à la décision.