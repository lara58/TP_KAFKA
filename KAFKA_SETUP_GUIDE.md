# Guide Kafka - Exercice 1 : Création du topic weather_stream

## Qu'est-ce qu'un topic Kafka ?

Un **topic** Kafka est comme une **boîte aux lettres** où les messages sont stockés.
- Les **producteurs** y envoient des messages
- Les **consommateurs** y lisent des messages

## Commande pour créer le topic weather_stream

```bash
docker exec kafka kafka-topics --create --topic weather_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Résultat de l'exercice 1

![Capture de l'exercice 1](exo.png)

Le screenshot montre :
- La création du topic `weather_stream`
- L'envoi du message `{"msg": "Hello Kafka"}`
- La lecture du message depuis le topic