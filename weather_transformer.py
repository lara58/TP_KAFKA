#!/usr/bin/env python3
"""
Exercice 7 - Transformateur de données météo
Lit weather_stream et produit weather_transformed avec alertes
"""

import json
import sys
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherTransformer:
    def __init__(self):
        # Consumer pour lire weather_stream
        self.consumer = KafkaConsumer(
            'weather_stream',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='weather_transformer_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Producer pour écrire weather_transformed
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Seuils d'alertes
        self.alert_thresholds = {
            'temperature_high': 35.0,  # > 35°C
            'temperature_low': -10.0,  # < -10°C
            'humidity_high': 85.0,     # > 85%
            'wind_high': 50.0         # > 50 km/h
        }

    def generate_alerts(self, weather_data):
        """Génère les alertes basées sur les données météo"""
        alerts = []
        temp = weather_data.get('temperature', 0)
        humidity = weather_data.get('humidity', 0)
        wind_speed = weather_data.get('wind_speed', 0)
        
        # Alerte température élevée
        if temp > self.alert_thresholds['temperature_high']:
            alerts.append({
                'type': 'TEMPERATURE_HIGH',
                'severity': 'HIGH',
                'message': f'Température très élevée: {temp}°C',
                'threshold': self.alert_thresholds['temperature_high'],
                'value': temp
            })
        
        # Alerte température basse
        if temp < self.alert_thresholds['temperature_low']:
            alerts.append({
                'type': 'TEMPERATURE_LOW',
                'severity': 'HIGH',
                'message': f'Température très basse: {temp}°C',
                'threshold': self.alert_thresholds['temperature_low'],
                'value': temp
            })
        
        # Alerte humidité élevée
        if humidity > self.alert_thresholds['humidity_high']:
            alerts.append({
                'type': 'HUMIDITY_HIGH',
                'severity': 'MEDIUM',
                'message': f'Humidité très élevée: {humidity}%',
                'threshold': self.alert_thresholds['humidity_high'],
                'value': humidity
            })
        
        # Alerte vent fort
        if wind_speed > self.alert_thresholds['wind_high']:
            alerts.append({
                'type': 'WIND_HIGH',
                'severity': 'HIGH',
                'message': f'Vent très fort: {wind_speed} km/h',
                'threshold': self.alert_thresholds['wind_high'],
                'value': wind_speed
            })
        
        return alerts

    def transform_message(self, original_message):
        """Transforme le message original en ajoutant les alertes"""
        alerts = self.generate_alerts(original_message)
        
        transformed = {
            # Données originales
            'city': original_message.get('city'),
            'country': original_message.get('country'),
            'latitude': original_message.get('latitude'),
            'longitude': original_message.get('longitude'),
            'temperature': original_message.get('temperature'),
            'humidity': original_message.get('humidity'),
            'wind_speed': original_message.get('wind_speed'),
            'original_timestamp': original_message.get('timestamp'),
            
            # Données transformées
            'transformation_timestamp': datetime.now().isoformat(),
            'alerts': alerts,
            'alert_count': len(alerts),
            'has_alerts': len(alerts) > 0,
            'max_severity': self.get_max_severity(alerts)
        }
        
        return transformed

    def get_max_severity(self, alerts):
        """Détermine la sévérité maximale des alertes"""
        if not alerts:
            return 'NONE'
        
        severities = [alert['severity'] for alert in alerts]
        if 'HIGH' in severities:
            return 'HIGH'
        elif 'MEDIUM' in severities:
            return 'MEDIUM'
        else:
            return 'LOW'

    def run(self):
        """Lance le transformateur"""
        print("Démarrage du transformateur météo...")
        print("Lecture de 'weather_stream' -> Écriture vers 'weather_transformed'")
        print("Seuils d'alertes configurés:")
        for key, value in self.alert_thresholds.items():
            print(f"  {key}: {value}")
        print("\nEn attente de messages... (Ctrl+C pour arrêter)")
        
        try:
            message_count = 0
            for message in self.consumer:
                try:
                    original_data = message.value
                    transformed_data = self.transform_message(original_data)
                    
                    # Envoie le message transformé
                    self.producer.send('weather_transformed', transformed_data)
                    self.producer.flush()
                    
                    message_count += 1
                    print(f"\nMessage {message_count} transformé:")
                    print(f"  Ville: {transformed_data['city']}, {transformed_data['country']}")
                    print(f"  Alertes: {transformed_data['alert_count']}")
                    if transformed_data['has_alerts']:
                        print(f"  Sévérité max: {transformed_data['max_severity']}")
                        for alert in transformed_data['alerts']:
                            print(f"    - {alert['type']}: {alert['message']}")
                    
                except Exception as e:
                    logger.error(f"Erreur lors de la transformation: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print(f"\nTransformateur arrêté. {message_count} messages traités.")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    transformer = WeatherTransformer()
    transformer.run()