# Exercice 2 : Consommateur Kafka en Python

## Qu'est-ce qu'un consommateur Kafka ?

Un **consommateur** Kafka est un programme qui lit les messages depuis un topic.
- Il **s'abonne** à un ou plusieurs topics
- Il **lit les messages** en continu
- Il **traite** les données reçues

**Rôle** : Le consommateur permet de récupérer et traiter les données stockées dans Kafka en temps réel.

## Description
Script Python qui lit les messages depuis un topic Kafka passé en argument et les affiche en temps réel.

## Comment fonctionne le script ?

1. **Vérifie l'argument** : Le script attend un nom de topic en paramètre
2. **Se connecte à Kafka** : Établit la connexion sur localhost:9092  
3. **S'abonne au topic** : Utilise un groupe de consommateurs unique
4. **Lit en boucle** : Poll toutes les secondes pour récupérer les messages
5. **Affiche les messages** : Format "topic: key = X value = Y"
6. **Gère l'arrêt** : Ctrl+C ferme proprement la connexion

## Utilisation

```bash
python consumer.py <topic_name>
```

## Exemple

```bash
python consumer.py weather_stream
```

## Résultat de l'exercice 2
![Capture de l'exercice 2](exo2.png)
Le screenshot montre :
- Le script consumer.py en action
- Lecture des messages depuis le topic weather_stream
- Affichage en temps réel des messages reçus



