#!/usr/bin/env python3
"""
Exercice 4 : Création de topic avec partitions
Script pour créer un topic Kafka avec plusieurs partitions pour démontrer les consumer groups
"""

import sys
import subprocess
import time

def create_partitioned_topic(topic_name, partitions=3, replication_factor=1):
    """Crée un topic Kafka avec le nombre de partitions spécifié"""
    
    print(f"Création du topic '{topic_name}' avec {partitions} partitions...")
    
    try:
        # Commande pour créer le topic avec partitions
        cmd = [
            "docker", "exec", "kafka",
            "kafka-topics", "--create",
            "--topic", topic_name,
            "--bootstrap-server", "localhost:9092",
            "--partitions", str(partitions),
            "--replication-factor", str(replication_factor)
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"Topic '{topic_name}' créé avec succès!")
        else:
            if "already exists" in result.stderr:
                print(f"Topic '{topic_name}' existe déjà")
            else:
                print(f"Erreur lors de la création: {result.stderr}")
                return False
                
    except Exception as e:
        print(f"Erreur: {e}")
        return False
    
    # Vérifier la création et afficher les détails
    time.sleep(2)
    describe_topic(topic_name)
    return True

def describe_topic(topic_name):
    """Affiche les détails du topic (partitions, réplication, etc.)"""
    
    print(f"\nDétails du topic '{topic_name}':")
    
    try:
        cmd = [
            "docker", "exec", "kafka",
            "kafka-topics", "--describe",
            "--topic", topic_name,
            "--bootstrap-server", "localhost:9092"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"Erreur lors de la description: {result.stderr}")
            
    except Exception as e:
        print(f"Erreur: {e}")

def list_topics():
    """Liste tous les topics disponibles"""
    
    print("\nTopics disponibles:")
    
    try:
        cmd = [
            "docker", "exec", "kafka",
            "kafka-topics", "--list",
            "--bootstrap-server", "localhost:9092"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            topics = result.stdout.strip().split('\n')
            for i, topic in enumerate(topics, 1):
                if topic.strip():
                    print(f"  {i}. {topic}")
        else:
            print(f"Erreur: {result.stderr}")
            
    except Exception as e:
        print(f"Erreur: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python setup_partitions.py <topic_name> [nb_partitions]")
        print("Exemple: python setup_partitions.py weather_partitioned 4")
        print("\nCommandes disponibles:")
        print("  python setup_partitions.py list              # Lister tous les topics")
        print("  python setup_partitions.py describe <topic>  # Décrire un topic")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "list":
        list_topics()
    elif command == "describe" and len(sys.argv) > 2:
        describe_topic(sys.argv[2])
    else:
        topic_name = command
        partitions = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        
        print("Configuration d'un topic Kafka avec partitions")
        print("=" * 50)
        
        if create_partitioned_topic(topic_name, partitions):
            print(f"\nLe topic '{topic_name}' est prêt pour l'exercice 4!")
            print(f"Vous pouvez maintenant tester les consumer groups avec:")
            print(f"   python consumer_group.py {topic_name} my_group 3")
            print(f"   python producer.py {topic_name}  # (depuis l'exercice 3)")

if __name__ == "__main__":
    main()