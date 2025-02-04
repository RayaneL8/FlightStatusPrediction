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
                    dbc.Col([
                        dcc.Graph(
                            id="animated-numeric-radar-plot",
                            figure=create_radar_plots_numeric(
                                all_metrics=all_metrics
                            )
                        ),
                    ], width=6),
                    dbc.Col([
                        dcc.Graph(
                            id="animated-percentage-radar-plot",
                            figure=create_radar_plots_percentage(
                                all_metrics=all_metrics
                            )
                        ),
                    ], width=6)
                ],
                style={"marginTop": "32px"}
            ),
            dbc.Row(
                children=[
                    dbc.Col([
                        dcc.Graph(
                            id="animated-numeric-barchart-plot",
                            figure=create_animated_bar_chart_numeric(
                                all_metrics=all_metrics
                            )
                        ),
                    ], width=6),
                    dbc.Col([
                        dcc.Graph(
                            id="animated-percentage-barchart-plot",
                            figure=create_animated_bar_chart_percentage(
                                all_metrics=all_metrics
                            )
                        ),
                    ], width=6)
                ],
                style={"marginTop": "32px"}
            ),
            dbc.Row(
                children=[
                    dcc.Graph(
                        id="animated-barchart-plot",
                        figure=generate_us_heatmap(
                            us_map_df=us_map_metrics
                        )
                    ),
                ],
                style={"marginTop": "32px"}
            )
        ],
        style={"marginTop": "64px"}
    )

#RADAR

def create_radar_plots_numeric(all_metrics):
    """
    Génère deux radar plots distincts :
    1. Un pour les catégories numériques (`avg_departure_delay`, `avg_arrival_delay`).
    2. Un pour les catégories en pourcentage (`cancelled_percentage`, `diverted_percentage`, etc.).
    """
    numeric_metrics = all_metrics[all_metrics["Parameter"].isin(["Avg_Departure_Delay", "Avg_Arrival_Delay"])]

    # Radar plot pour les métriques numériques
    numeric_radar_fig = px.line_polar(
        numeric_metrics,
        r="Value",
        theta="Parameter",
        color="Airline",
        line_close=True,
        animation_frame="Year",
        title="Radar Plot (Numeric Metrics)",
        template="plotly_dark"
    )
    numeric_radar_fig.update_layout(
        polar=dict(radialaxis=dict(visible=True)),
        legend_title="Compagnies Aériennes"
    )

    return numeric_radar_fig

def create_radar_plots_percentage(all_metrics):
    """
    Génère deux radar plots distincts :
    1. Un pour les catégories numériques (`avg_departure_delay`, `avg_arrival_delay`).
    2. Un pour les catégories en pourcentage (`cancelled_percentage`, `diverted_percentage`, etc.).
    """
    percentage_metrics = all_metrics[~all_metrics["Parameter"].isin(["Avg_Departure_Delay", "Avg_Arrival_Delay"])]

    # Radar plot pour les métriques en pourcentage
    percentage_radar_fig = px.line_polar(
        percentage_metrics,
        r="Value",
        theta="Parameter",
        color="Airline",
        line_close=True,
        animation_frame="Year",
        title="Radar Plot (Percentage Metrics)",
        template="plotly_dark"
    )
    percentage_radar_fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        legend_title="Compagnies Aériennes"
    )

    return percentage_radar_fig


#ANIMATED BARCHART
def create_animated_bar_chart_numeric(all_metrics):
    """
    Génère deux bar charts animés distincts :
    1. Un pour les catégories numériques (`avg_departure_delay`, `avg_arrival_delay`).
    2. Un pour les catégories en pourcentage (`cancelled_percentage`, `diverted_percentage`, etc.).
    """
    # Séparer les catégories
    numeric_metrics = all_metrics[all_metrics["Parameter"].isin(["Avg_Departure_Delay", "Avg_Arrival_Delay"])]

    # Bar chart pour les métriques numériques
    numeric_fig = px.bar(
        numeric_metrics.groupby(["Airline", "Year", "Parameter"], as_index=False).mean(),
        x="Parameter",
        y="Value",
        color="Airline",
        animation_frame="Year",
        title="Bar Chart (Numeric Metrics)",
        template="plotly_white"
    )
    numeric_fig.update_layout(
        xaxis_title="Paramètres",
        yaxis_title="Valeur (en minutes)",
        legend_title="Compagnies Aériennes",
        xaxis_tickangle=-45,
        bargap=0.4,
        barmode="group",
        margin=dict(l=50, r=50, t=50, b=50)
    )
    numeric_fig.update_traces(
        texttemplate='%{y:.2f}',
        textposition="outside",
        opacity=0.9
    )

    return numeric_fig


def create_animated_bar_chart_percentage(all_metrics):
    """
    Génère deux bar charts animés distincts :
    1. Un pour les catégories numériques (`avg_departure_delay`, `avg_arrival_delay`).
    2. Un pour les catégories en pourcentage (`cancelled_percentage`, `diverted_percentage`, etc.).
    """
    # Séparer les catégories
    percentage_metrics = all_metrics[~all_metrics["Parameter"].isin(["Avg_Departure_Delay", "Avg_Arrival_Delay"])]

    # Bar chart pour les métriques en pourcentage
    percentage_fig = px.bar(
        percentage_metrics.groupby(["Airline", "Year", "Parameter"], as_index=False).mean(),
        x="Parameter",
        y="Value",
        color="Airline",
        animation_frame="Year",
        title="Bar Chart (Percentage Metrics)",
        template="plotly_white"
    )
    percentage_fig.update_layout(
        xaxis_title="Paramètres",
        yaxis_title="Pourcentage",
        legend_title="Compagnies Aériennes",
        xaxis_tickangle=-45,
        bargap=0.4,
        barmode="group",
        yaxis=dict(range=[0, 100]),
        margin=dict(l=50, r=50, t=50, b=50)
    )
    percentage_fig.update_traces(
        texttemplate='%{y:.2f}',
        textposition="outside",
        opacity=0.9
    )

    return percentage_fig





def generate_us_heatmap(us_map_df):
    """
    Génère une carte interactive avec une combinaison des retards moyens et des annulations.
    """
    if us_map_df.empty:
        print("Erreur : Le DataFrame est vide. Impossible de générer la carte.")
        return None

    # Combiner les deux valeurs en pondérant Total_Cancellations
    us_map_df["Impact_Score"] = us_map_df["Avg_Departure_Delay"] + (us_map_df["Total_Cancellations"] / 1000)

    fig = px.scatter_mapbox(
        us_map_df,
        lat="Latitude",
        lon="Longitude",
        color="Impact_Score",
        size="Impact_Score",
        hover_name="City",
        hover_data={"State": True, "Avg_Departure_Delay": True, "Total_Cancellations": True},
        color_continuous_scale="Viridis",
        size_max=15,
        zoom=3,
        title="Mapping of average delay duration and cancellations"
    )

    fig.update_layout(
        showlegend= True,
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        geo= dict(
            scope='usa',
            landcolor = 'rgb(217,217,217)'
        )
    )

    return fig








