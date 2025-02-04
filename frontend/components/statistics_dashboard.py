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
                        figure=generate_calendar_heatmaps(
                            data=data
                        )
                    ),
                ],
                style={"marginTop": "32px"}
            )
        ],
        style={"marginTop": "64px"}
    )


def generate_calendar_heatmaps(data):
    """ Génère les heatmaps calendrier pour les retards, détournements et annulations. """
    figs = {}
    categories = {
        "delay_calendar": "Delayed",
        "diverted_calendar": "Diverted",
        "cancelled_calendar": "Cancelled"
    }

    for key, category in categories.items():
        if key in data:  # Vérification que la clé existe bien dans la réponse JSON
            df = Cpreset_requests.prepare_calendar_data(data[key], category)

            fig = px.density_heatmap(df, x="Month_Name", y="Year", z="Percentage",
                                     title=f"{category} Calendar Heatmap",
                                     color_continuous_scale="Viridis")

            figs[category] = fig
            fig.show()
        else:
            print(f"Aucune donnée trouvée pour {category}")