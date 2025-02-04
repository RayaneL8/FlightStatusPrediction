from dash import html, dcc, dash_table
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(data):
    return create_simple_table(data_list=data)

def create_simple_table(data_list):
    """
    Crée un simple Dash DataTable affichant toutes les données.
    """
    # Transformer la liste en DataFrame
    df = pd.DataFrame(data_list)

    # Convertir les timestamps en chaînes lisibles
    if "FlightDate" in df.columns:
        df["FlightDate"] = df["FlightDate"].astype(str)

    return dash_table.DataTable(
        id="data-table",
        columns=[{"name": col, "id": col} for col in df.columns],
        data=df.to_dict("records"),  # Convertir les lignes en dictionnaires
        style_table={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"},
        style_cell={
            "textAlign": "left",
            "padding": "5px",
            "fontSize": 14,
            "fontFamily": "Arial",
            "whiteSpace": "normal",
        },
        style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"},
        page_size=10,  # Nombre de lignes affichées par page
    )