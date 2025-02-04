from dash import html, dcc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(data):
    """
    Génère le layout contenant les heatmaps calendrier à afficher verticalement.
    """
    calendar_heatmaps = generate_calendar_heatmaps(data)  # Appeler la fonction pour générer les heatmaps
    
    rows = []
    for category, fig in calendar_heatmaps.items():
        rows.append(
            dbc.Row(
                children=[
                    dcc.Graph(
                        id=f"{category.lower()}-heatmap",
                        figure=fig
                    )
                ],
                style={"marginTop": "32px"}
            )
        )

    return html.Div(
        children=rows,
        style={"marginTop": "64px"}
    )


def generate_calendar_heatmaps(data):
    """
    Génère les heatmaps calendrier pour les retards, détournements et annulations.
    Retourne un dictionnaire contenant les figures Plotly.
    """
    figs = {}
    categories = {
        "delay_calendar": "Delayed",
        "diverted_calendar": "Diverted",
        "cancelled_calendar": "Cancelled"
    }

    for key, category in categories.items():
        if key in data:  # Vérifie que la clé existe bien dans la réponse JSON
            df = Cpreset_requests.prepare_calendar_data(data[key], category)

            # Créer la heatmap avec Plotly
            fig = px.density_heatmap(
                df,
                x="Month_Name",  # Mois sur l'axe X
                y="Year",        # Années sur l'axe Y
                z="Percentage",  # Pourcentage des retards/annulations/détournements
                title=f"{category} Calendar Heatmap",
                color_continuous_scale="Viridis"
            )

            # Ajouter la figure dans le dictionnaire
            figs[category] = fig
        else:
            print(f"Aucune donnée trouvée pour {category}")

    return figs
