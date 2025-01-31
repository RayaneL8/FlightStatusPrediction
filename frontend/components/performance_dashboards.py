from dash import html, dcc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(all_metrics, cities, airlines):
    return html.Div(
        [
            dbc.Row(
                children=[
                    dbc.Col(
                        dcc.Graph(
                            id="radar-plot",
                            figure=generate_radar_dashboard(
                                all_metrics=all_metrics
                            ),
                        ),
                        width=6
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id="animated-barchart-plot",
                            figure=create_animated_bar_chart(
                                data=all_metrics
                            )
                        ),
                        width=6
                    ),
                    
                ]
            )
        ]
    )

#RADAR

def generate_radar_dashboard(all_metrics):
    categories = ["Avg_Departure_Delay", "Avg_Arrival_Delay", "Cancelled_Percentage", "Diverted_Percentage"]
    fig = go.Figure()

    print("Cadeau: ", all_metrics)
    fig = px.line_polar(
        all_metrics,
        r="Value",  # Rayon : les valeurs des paramètres
        theta="Parameter",  # Angle : les catégories
        color="Airline",  # Couleur pour chaque compagnie
        line_close=True,  # Ferme les lignes du radar
        animation_frame="Year",  # Animation basée sur l'année
        title="Radar Plot Animé des Métriques par Compagnie et Année",
        template="plotly_dark"
    )

    # Mise en forme
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        legend_title="Compagnies Aériennes"
    )

    return fig


#ANIMATED BARCHART
def create_animated_bar_chart(data):
    """
    Crée un graphique à barres animé en utilisant Plotly avec normalisation par catégorie.
    """
    if data.empty:
        print("Erreur : Le DataFrame est vide. Impossible de créer le graphique.")
        return None

    # Vérifier les données avant de créer le graphique
    print("Aperçu des données utilisées pour le graphique :")
    print(data.groupby("Parameter")["Value"].describe())

    # Normaliser les valeurs par catégorie
    data["Normalized_Value"] = data.groupby("Parameter")["Value"].transform(lambda x: (x / x.max()) * 100)

    # Création du graphique
    fig = px.bar(
        data,
        x="Parameter",  # Paramètres sur l'axe X
        y="Normalized_Value",  # Pourcentages normalisés sur l'axe Y
        color="Airline",  # Compagnies aériennes en couleur
        animation_frame="Year",  # Animation par année
        animation_group="Airline",  # Groupement par compagnie aérienne
        range_y=[0, 100],  # Plage fixe pour les pourcentages
        title="Animation des métriques par compagnie aérienne"
    )

    fig.update_layout(
        xaxis_title="Paramètres",
        yaxis_title="Pourcentage (Normalisé)",
        legend_title="Compagnies Aériennes",
        xaxis_tickangle=-45,  # Incliner les étiquettes pour plus de lisibilité
    )

    fig.update_traces(
        opacity=0.8,  # Ajuster la transparence des barres
        texttemplate='%{y:.2f}',  # Afficher les valeurs au-dessus des barres
        textposition='outside'
    )

    return fig

