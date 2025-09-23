#!/usr/bin/env python3
"""
Weather Visualizer (Version Plotly uniquement) pour l'exercice 8
Crée des visualisations interactives avec Plotly seulement
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime
import os

from hdfs_reader import HDFSLogsReader

class WeatherVisualizerPlotly:
    """Générateur de visualisations Plotly pour les données météo HDFS"""
    
    def __init__(self, hdfs_path: str = "./hdfs-data"):
        self.reader = HDFSLogsReader(hdfs_path)
        self.df = None
        self.geo_df = None
        self.output_dir = "visualizations"
        
        # Créer le répertoire de sortie
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Charger les données
        self.load_data()
        
    def load_data(self):
        """Charge les données depuis HDFS"""
        print("Chargement des données HDFS...")
        self.df = self.reader.to_dataframe()
        self.geo_df = self.reader.get_geographic_data()
        print(f"{len(self.df)} alertes chargées")
        
    def create_alerts_by_country(self):
        """Graphique en barres des alertes par pays"""
        country_counts = self.df['country'].value_counts()
        
        fig = go.Figure(data=[
            go.Bar(x=country_counts.index, y=country_counts.values,
                  marker_color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4'],
                  text=country_counts.values,
                  textposition='auto')
        ])
        
        fig.update_layout(
            title='Distribution des Alertes par Pays',
            xaxis_title='Pays',
            yaxis_title='Nombre d\'alertes',
            title_font_size=16,
            height=500
        )
        
        fig.write_html(f'{self.output_dir}/alerts_by_country.html')
        print(f"Sauvegardé: {self.output_dir}/alerts_by_country.html")
        return fig
        
    def create_severity_distribution(self):
        """Graphique en secteurs de la distribution des sévérités"""
        severity_counts = self.df['severity'].value_counts()
        colors = {'HIGH': '#FF4444', 'MEDIUM': '#FFA500', 'LOW': '#FFFF44'}
        plot_colors = [colors.get(severity, '#CCCCCC') for severity in severity_counts.index]
        
        fig = go.Figure(data=[
            go.Pie(labels=severity_counts.index, 
                  values=severity_counts.values,
                  marker_colors=plot_colors,
                  textinfo='label+percent',
                  textfont_size=12)
        ])
        
        fig.update_layout(
            title='Distribution des Sévérités d\'Alertes',
            title_font_size=16,
            height=500
        )
        
        fig.write_html(f'{self.output_dir}/severity_distribution.html')
        print(f"Sauvegardé: {self.output_dir}/severity_distribution.html")
        return fig
        
    def create_alert_types_heatmap(self):
        """Heatmap des types d'alertes par pays"""
        # Créer une matrice de comptage
        alert_matrix = self.df.groupby(['country', 'alert_type']).size().unstack(fill_value=0)
        
        fig = go.Figure(data=go.Heatmap(
            z=alert_matrix.values,
            x=alert_matrix.columns,
            y=alert_matrix.index,
            colorscale='Reds',
            text=alert_matrix.values,
            texttemplate="%{text}",
            textfont={"size":12},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title='Heatmap des Types d\'Alertes par Pays',
            xaxis_title='Type d\'alerte',
            yaxis_title='Pays',
            title_font_size=16,
            height=500
        )
        
        fig.write_html(f'{self.output_dir}/alert_types_heatmap.html')
        print(f"Sauvegardé: {self.output_dir}/alert_types_heatmap.html")
        return fig
        
    def create_world_map(self):
        """Carte mondiale interactive avec les alertes"""
        fig = px.scatter_geo(self.geo_df,
                           lat='latitude',
                           lon='longitude',
                           size='total_alerts',
                           color='avg_temperature',
                           hover_name='city',
                           hover_data={
                               'country': True,
                               'total_alerts': True,
                               'avg_temperature': ':.1f',
                               'avg_humidity': ':.1f',
                               'avg_wind_speed': ':.1f'
                           },
                           color_continuous_scale='RdYlBu_r',
                           size_max=50,
                           title='Carte Mondiale des Alertes Météo')
        
        fig.update_layout(
            geo=dict(
                showframe=False,
                showcoastlines=True,
                projection_type='equirectangular'
            ),
            title_font_size=16,
            height=600
        )
        
        fig.write_html(f'{self.output_dir}/world_map.html')
        print(f"Sauvegardé: {self.output_dir}/world_map.html")
        return fig
        
    def create_temperature_wind_scatter(self):
        """Scatter plot température vs vent avec sévérité"""
        severity_colors = {'HIGH': 'red', 'MEDIUM': 'orange', 'LOW': 'green'}
        
        fig = px.scatter(self.df, 
                        x='temperature', 
                        y='wind_speed',
                        color='severity',
                        size='humidity',
                        hover_name='city',
                        hover_data=['country', 'alert_type'],
                        color_discrete_map=severity_colors,
                        title='Température vs Vitesse du Vent')
        
        # Ajouter des lignes de seuils
        fig.add_hline(y=50, line_dash="dash", line_color="blue", 
                     annotation_text="Seuil vent fort (50 km/h)")
        fig.add_vline(x=35, line_dash="dash", line_color="red", 
                     annotation_text="Seuil chaleur (35°C)")
        fig.add_vline(x=-10, line_dash="dash", line_color="blue", 
                     annotation_text="Seuil froid (-10°C)")
        
        fig.update_layout(
            xaxis_title='Température (°C)',
            yaxis_title='Vitesse du vent (km/h)',
            title_font_size=16,
            height=600
        )
        
        fig.write_html(f'{self.output_dir}/temperature_wind_scatter.html')
        print(f"Sauvegardé: {self.output_dir}/temperature_wind_scatter.html")
        return fig
        
    def create_weather_metrics_dashboard(self):
        """Dashboard interactif avec toutes les métriques météo"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Alertes par Pays', 'Types d\'Alertes', 
                           'Température par Ville', 'Humidité par Ville'),
            specs=[[{"type": "bar"}, {"type": "pie"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # 1. Alertes par pays
        country_counts = self.df['country'].value_counts()
        fig.add_trace(
            go.Bar(x=country_counts.index, y=country_counts.values,
                  name="Alertes par pays",
                  marker_color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']),
            row=1, col=1
        )
        
        # 2. Types d'alertes (pie chart)
        alert_counts = self.df['alert_type'].value_counts()
        fig.add_trace(
            go.Pie(labels=alert_counts.index, values=alert_counts.values,
                  name="Types d'alertes"),
            row=1, col=2
        )
        
        # 3. Température par ville
        fig.add_trace(
            go.Bar(x=self.df['city'], y=self.df['temperature'],
                  name="Température",
                  marker_color='orange'),
            row=2, col=1
        )
        
        # 4. Humidité par ville
        fig.add_trace(
            go.Bar(x=self.df['city'], y=self.df['humidity'],
                  name="Humidité",
                  marker_color='lightblue'),
            row=2, col=2
        )
        
        fig.update_layout(
            title_text="Dashboard Météorologique Complet",
            title_font_size=18,
            height=800,
            showlegend=False
        )
        
        fig.write_html(f'{self.output_dir}/weather_dashboard.html')
        print(f"Sauvegardé: {self.output_dir}/weather_dashboard.html")
        return fig
        
    def create_timeline_plot(self):
        """Timeline des alertes"""
        # Convertir les timestamps en minutes depuis la première alerte
        min_time = self.df['original_time'].min()
        self.df['minutes_elapsed'] = (self.df['original_time'] - min_time).dt.total_seconds() / 60
        
        # Utiliser la valeur absolue de la température pour la taille + offset
        self.df['temp_abs'] = abs(self.df['temperature']) + 10
        
        severity_colors = {'HIGH': 'red', 'MEDIUM': 'orange', 'LOW': 'green'}
        
        fig = px.scatter(self.df, 
                        x='minutes_elapsed', 
                        y='city',
                        color='severity',
                        size='temp_abs',
                        hover_data=['alert_type', 'country', 'temperature'],
                        color_discrete_map=severity_colors,
                        title='Timeline des Alertes Météo')
        
        fig.update_layout(
            xaxis_title='Temps (minutes depuis la première alerte)',
            yaxis_title='Ville',
            title_font_size=16,
            height=500
        )
        
        fig.write_html(f'{self.output_dir}/alerts_timeline.html')
        print(f"Sauvegardé: {self.output_dir}/alerts_timeline.html")
        return fig
        
    def create_summary_table(self):
        """Table récapitulative des données"""
        summary_data = []
        for _, row in self.df.iterrows():
            summary_data.append({
                'Pays': row['country'],
                'Ville': row['city'],
                'Type d\'alerte': row['alert_type'],
                'Sévérité': row['severity'],
                'Température': f"{row['temperature']:.1f}°C",
                'Humidité': f"{row['humidity']:.0f}%",
                'Vent': f"{row['wind_speed']:.1f} km/h",
                'Seuil': row['threshold'],
                'Valeur': row['value']
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        fig = go.Figure(data=[go.Table(
            header=dict(values=list(summary_df.columns),
                       fill_color='paleturquoise',
                       align='left',
                       font=dict(size=12)),
            cells=dict(values=[summary_df[col] for col in summary_df.columns],
                      fill_color='lavender',
                      align='left',
                      font=dict(size=11)))
        ])
        
        fig.update_layout(
            title='Résumé Détaillé des Alertes',
            title_font_size=16,
            height=400
        )
        
        fig.write_html(f'{self.output_dir}/summary_table.html')
        print(f"Sauvegardé: {self.output_dir}/summary_table.html")
        return fig
        
    def generate_all_visualizations(self):
        """Génère toutes les visualisations"""
        print("\nGÉNÉRATION DES VISUALISATIONS - EXERCICE 8")
        print("="*60)
        
        # Créer tous les graphiques
        self.create_alerts_by_country()
        self.create_severity_distribution()
        self.create_alert_types_heatmap()
        self.create_world_map()
        self.create_temperature_wind_scatter()
        self.create_timeline_plot()
        self.create_summary_table()
        self.create_weather_metrics_dashboard()
        
        print(f"\nToutes les visualisations générées dans: {self.output_dir}/")
        
        # Lister les fichiers créés
        files = os.listdir(self.output_dir)
        print("\nFichiers créés:")
        for file in sorted(files):
            print(f"   • {file}")
            
    def display_summary_stats(self):
        """Affiche les statistiques résumées"""
        print("\nSTATISTIQUES RÉSUMÉES")
        print("="*40)
        print(f"Nombre total d'alertes: {len(self.df)}")
        print(f"Pays couverts: {', '.join(self.df['country'].unique())}")
        print(f"Villes: {', '.join(self.df['city'].unique())}")
        
        print(f"\nTempératures:")
        print(f"   • Min: {self.df['temperature'].min():.1f}°C")
        print(f"   • Max: {self.df['temperature'].max():.1f}°C")
        print(f"   • Moyenne: {self.df['temperature'].mean():.1f}°C")
        
        print(f"\nVitesses de vent:")
        print(f"   • Min: {self.df['wind_speed'].min():.1f} km/h")
        print(f"   • Max: {self.df['wind_speed'].max():.1f} km/h")
        print(f"   • Moyenne: {self.df['wind_speed'].mean():.1f} km/h")
        
        print(f"\nHumidité:")
        print(f"   • Min: {self.df['humidity'].min():.1f}%")
        print(f"   • Max: {self.df['humidity'].max():.1f}%")
        print(f"   • Moyenne: {self.df['humidity'].mean():.1f}%")

if __name__ == "__main__":
    # Créer le visualiseur
    visualizer = WeatherVisualizerPlotly()
    
    # Afficher les statistiques
    visualizer.display_summary_stats()
    
    # Générer toutes les visualisations
    visualizer.generate_all_visualizations()
    
    print(f"\nEXERCICE 8 TERMINÉ!")
    print(f"Dashboard complet disponible dans: visualizations/")
    print(f"Carte interactive: visualizations/world_map.html")
    print(f"Dashboard interactif: visualizations/weather_dashboard.html")
    print(f"Table récapitulative: visualizations/summary_table.html")