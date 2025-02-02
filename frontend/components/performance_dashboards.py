from dash import html, dcc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(all_metrics, us_map_metrics, cities, airlines):
    return html.Div(
        [
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="radar-plot",
                        figure=generate_radar_dashboard(
                            all_metrics=all_metrics
                        ),
                    ),
                ]
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="animated-barchart-plot",
                        figure=create_animated_bar_chart(
                            all_metrics=all_metrics
                        )
                    ),
                ]
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="animated-barchart-plot",
                        figure=generate_us_heatmap(
                            us_map_df=us_map_metrics
                        )
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
def create_animated_bar_chart(all_metrics):
    """
    Génère un bar chart animé avec des barres non empilées (côte à côte) 
    et gère les duplications des données.
    """
    # Suppression des duplications
    all_metrics = (
        all_metrics.groupby(["Airline", "Year", "Parameter"], as_index=False)
        .mean()  # Moyenne des valeurs si des duplications existent
    )

    # Création de la figure
    fig = px.bar(
        all_metrics,
        x="Parameter",  # Paramètres sur l'axe X
        y="Value",  # Valeurs des métriques
        color="Airline",  # Couleurs par compagnie aérienne
        animation_frame="Year",  # Animation par année
        title="Bar Chart Animé des Métriques par Compagnie et Année",
        template="plotly_white"  # Style visuel clair
    )

    # Mise en forme des barres
    fig.update_layout(
        xaxis_title="Paramètres",
        yaxis_title="Pourcentage",
        legend_title="Compagnies Aériennes",
        xaxis_tickangle=-45,  # Inclinaison des étiquettes des paramètres
        bargap=0.4,  # Espacement entre les barres
        barmode="group",  # Afficher les barres côte à côte
        yaxis=dict(range=[0, 100]),  # Limites de l'axe Y entre 0 et 100
        margin=dict(l=50, r=50, t=50, b=50)
    )

    # Ajouter les valeurs au-dessus des barres
    fig.update_traces(
        texttemplate='%{y:.2f}',  # Afficher les valeurs avec 2 décimales
        textposition="outside",  # Positionner les valeurs au-dessus des barres
        opacity=0.9  # Rendre les barres légèrement transparentes
    )

    return fig

def generate_us_heatmap(us_map_df):
    """
    Génère une carte heatmap des États-Unis pour visualiser les retards moyens au départ et les annulations.
    """
    if us_map_df.empty or "Latitude" not in us_map_df.columns or "Longitude" not in us_map_df.columns:
        print("Le DataFrame est vide ou manque de coordonnées géographiques.")
        return None

    # Création de la carte avec les coordonnées GPS
    fig = px.scatter_geo(
        us_map_df,
        lat="Latitude",
        lon="Longitude",
        hover_name="City",
        hover_data={
            "State": True,
            "Avg_Departure_Delay": ":.2f",
            "Total_Cancellations": True
        },
        size="Total_Cancellations",
        color="Avg_Departure_Delay",
        color_continuous_scale=px.colors.sequential.Viridis,
        title="Heatmap des Retards Moyens et Annulations par Ville"
    )

    # Ajustements esthétiques
    fig.update_layout(
        geo=dict(
            scope="usa",
            showland=True,
            landcolor="rgb(217, 217, 217)",
            subunitcolor="rgb(255, 255, 255)",
            countrycolor="rgb(255, 255, 255)"
        ),
        coloraxis_colorbar=dict(
            title="Retard Moyen",
            ticks="outside"
        )
    )

    return fig










