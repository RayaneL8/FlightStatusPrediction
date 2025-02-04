from dash import html, dcc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(data):
    return html.Div(
        [
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="animated-barchart-plot",
                        figure=generate_bar_chart(
                            df=data
                        )
                    ),
                ],
                style={"marginTop": "32px"}
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="animated-radar-plot",
                        figure=generate_radar(
                            df=data
                        )
                    ),
                ],
                style={"marginTop": "32px"}
            )
        ],
        style={"marginTop": "64px"}
    )


def generate_bar_chart(df):
    # Agréger les données pour chaque combinaison unique de Parameter, Airline et Year
    aggregated_df = (
        df.groupby(["Parameter", "Airline", "Year"], as_index=False)
        .mean()  # Calculer la moyenne des valeurs pour chaque groupe
    )

    # Créer le graphique à barres
    bar_chart_fig = px.bar(
        aggregated_df,
        x="Parameter",
        y="Value",
        color="Airline",
        animation_frame="Year",
        title="Bar Chart displaying statistics of Airlines per Years",
        template="plotly_white"
    )

    # Mise en forme du graphique
    bar_chart_fig.update_layout(
        xaxis_tickangle=-45,  # Inclinaison des étiquettes des paramètres
        bargap=0.4,  # Espacement entre les barres
        barmode="group"  # Afficher les barres côte à côte
    )

    # Ajouter les valeurs au-dessus des barres
    bar_chart_fig.update_traces(
        texttemplate='%{y:.2f}',  # Afficher les valeurs avec 2 décimales
        textposition="outside",  # Positionner les valeurs au-dessus des barres
        opacity=0.9  # Rendre les barres légèrement transparentes
    )

    return bar_chart_fig


def generate_radar(df):
    radar_fig = px.line_polar(
        df,
        r="Value",
        theta="Parameter",
        color="Airline",
        line_close=True,
        animation_frame="Year",
        title="Radar Plot displaying statistics of Airlines per Years",
        template="plotly_dark"
    )
    return radar_fig