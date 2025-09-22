# Exercice 3 : Producer Kafka en Python

## Objectif
Créer un **Producer** Python qui envoie des données météo vers un topic Kafka.

## Qu'est-ce qu'un Producer Kafka ?
Un **Producer** est une application qui envoie des messages vers un ou plusieurs topics Kafka. Il est l'expéditeur dans le système de messagerie Kafka.

Le Producer de cet exercice :
- Récupère des données météo de 5 villes françaises via l'API Open-Meteo
- Envoie 10 messages météo vers le topic spécifié
- Affiche les confirmations d'envoi avec les informations de partition et offset

## Fonctionnement du script `producer.py`

Le script suit ces étapes :

1. **Validation des arguments** : Vérifie qu'un nom de topic est fourni
2. **Configuration du Producer** : Connexion à Kafka sur `localhost:9092`
3. **Génération des données** : 
   - Sélection aléatoire d'une ville parmi Paris, Lyon, Marseille, Toulouse, Nice
   - Récupération des données météo via l'API Open-Meteo
   - Formatage des données avec timestamp, température, humidité, vitesse du vent
4. **Envoi vers Kafka** : Publication du message au format JSON
5. **Confirmation** : Affichage des informations de livraison (partition, offset)
6. **Répétition** : Envoi de 10 messages au total avec une pause d'1 seconde entre chaque

## Résumé des commandes

### 1. Démarrer Kafka (si pas déjà fait)
```bash
docker-compose up -d
```

### 2. Vérifier que Kafka fonctionne
```bash
docker-compose ps
```

### 3. Lancer le producer
```bash
python producer.py weather_stream
```

### 4. Vérifier les messages (optionnel)
```bash
docker exec kafka kafka-console-consumer --topic weather_stream --bootstrap-server localhost:9092 --from-beginning --max-messages 5
```

### 5. Tester avec le consumer de l'exercice 2
```bash
# Basculer vers la branche exo2
git checkout exo2
# Lancer le consumer
python consumer.py weather_stream
```

## Captures d'écran
- **exo3.png** : Capture du producer en action envoyant les messages météo
- **exo3_consumer.png** : Capture du consumer recevant les messages du producer

## Sortie attendue du Producer
```
Démarrage du producer météo pour le topic: weather_stream
Envoi de 10 messages météo...
Message 1/10 envoyé - Lyon: 15.1°C
  Partition: 0, Offset: 2
Message 2/10 envoyé - Marseille: 19.9°C
  Partition: 0, Offset: 3
...
Tous les messages ont été envoyés!
Vous pouvez maintenant lancer le consumer pour les lire:
python consumer.py weather_stream
```

## Test complet Producer ↔ Consumer
1. **Terminal 1** : Lancer le producer pour envoyer les données
2. **Terminal 2** : Lancer le consumer pour recevoir les données
3. Vérifier que les données météo transitent correctement entre les deux

## Structure des données envoyées
Chaque message contient :
```json
{
  "city": "Paris",
  "timestamp": "2025-09-22T14:30:15.123456",
  "temperature": 18.5,
  "humidity": 65,
  "wind_speed": 12,
  "message_id": 1
}
```

## Dépendances
- `kafka-python` : Client Kafka pour Python
- `requests` : Pour les appels à l'API météo

Installation :
```bash
pip install kafka-python requests
```