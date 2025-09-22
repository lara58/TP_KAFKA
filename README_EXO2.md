# Exercice 2 : Consommateur Kafka en Python

## Qu'est-ce qu'un consommateur Kafka ?

Un **consommateur** Kafka est un programme qui lit les messages depuis un topic.
- Il **s'abonne** ├á un ou plusieurs topics
- Il **lit les messages** en continu
- Il **traite** les donn├®es re├ºues

**R├┤le** : Le consommateur permet de r├®cup├®rer et traiter les donn├®es stock├®es dans Kafka en temps r├®el.

## Description
Script Python qui lit les messages depuis un topic Kafka pass├® en argument et les affiche en temps r├®el.

## Comment fonctionne le script ?

1. **V├®rifie l'argument** : Le script attend un nom de topic en param├¿tre
2. **Se connecte ├á Kafka** : ├ëtablit la connexion sur localhost:9092  
3. **S'abonne au topic** : Utilise un groupe de consommateurs unique
4. **Lit en boucle** : Poll toutes les secondes pour r├®cup├®rer les messages
5. **Affiche les messages** : Format "topic: key = X value = Y"
6. **G├¿re l'arr├¬t** : Ctrl+C ferme proprement la connexion

## Utilisation

```bash
python consumer.py <topic_name>
```

## Exemple

```bash
python consumer.py weather_stream
```

## R├®sultat de l'exercice 2
![Capture de l'exercice 2](exo2.png)
Le screenshot montre :
- Le script consumer.py en action
- Lecture des messages depuis le topic weather_stream
- Affichage en temps r├®el des messages re├ºus



