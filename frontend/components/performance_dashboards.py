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
                            )
                        ),
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id="animated-barchart-plot",
                            figure=create_animated_bar_chart(
                                data=Cpreset_requests.transform_to_dataframe(start_year=2018, end_year=2022, cities=cities, airlines=airlines)
                            )
                        ),
                    )
                ]
            )
        ]
    )

#RADAR

def generate_radar_dashboard(all_metrics):
    categories = ["Avg_Departure_Delay", "Avg_Arrival_Delay", "Cancelled_Percentage", "Diverted_Percentage"]
    fig = go.Figure()

    for airline, metrics in all_metrics.items():
        values = [metrics[cat] for cat in categories]
        fig.add_trace(
            go.Scatterpolar(
                r=values,
                theta=categories,
                fill="toself",
                name=airline,
            )
        )

    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100]),
        ),
        showlegend=True,
        title="Radar Plot des métriques par compagnie aérienne",
    )

    return fig


#ANIMATED BARCHART
def create_animated_bar_chart(data):
    """
    Crée un graphique animé à partir du DataFrame structuré.
    """
    if data.empty:
        print("Le DataFrame est vide. Impossible de créer le graphique.")
        return None

    fig = px.bar(
        data,
        x="Parameter",
        y="Value",
        color="Airline",
        animation_frame="Year",
        animation_group="Airline",
        range_y=[0, 100],
        title="Animation des métriques par compagnie aérienne"
    )

    fig.update_layout(
        xaxis_title="Paramètres",
        yaxis_title="Pourcentage",
        legend_title="Compagnies Aériennes",
    )
    fig.update_traces(texttemplate='%{y:.2f}', textposition='outside')

    return fig
