from dash import html, dcc, dash_table
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(data):
    return create_heatmap_or_table(data_list=data)

def create_heatmap_or_table(data_list):
    """
    Crée une heatmap si une colonne numérique est détectée, sinon affiche un tableau.
    """
    # Transformer la liste en DataFrame
    df = pd.DataFrame(data_list)
    
    # Identifier les colonnes numériques
    numeric_cols = df.select_dtypes(include=['number']).columns

    # Si aucune colonne numérique n'est détectée, renvoyer un tableau Dash DataTable
    if len(numeric_cols) == 0:
        return dash_table.DataTable(
            id="data-table",
            columns=[{"name": col, "id": col} for col in df.columns],
            data=df.to_dict("records"),  # Convertir les lignes en dictionnaires
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "5px",
                "fontSize": 14,
                "fontFamily": "Arial",
            },
            style_header={"backgroundColor": "lightgrey", "fontWeight": "bold"}
        )

    # Si une colonne numérique est détectée, créer une heatmap
    numeric_col = numeric_cols[0]  # Utiliser la première colonne numérique trouvée
    category_cols = df.select_dtypes(exclude=['number']).columns
    if len(category_cols) == 0:
        raise ValueError("Aucune colonne catégorielle détectée pour l'axe X/Y.")

    category_col = category_cols[0]  # Utiliser la première colonne catégorielle trouvée
    fig = px.density_heatmap(
        df,
        x=category_col,
        y=category_col,
        z=numeric_col,
        color_continuous_scale="Viridis",
        title=f"Heatmap ({numeric_col} par {category_col})",
        template="plotly_white"
    )

    return dcc.Graph(id="heatmap-plot", figure=fig)