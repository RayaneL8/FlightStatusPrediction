from dash import html, dcc
import plotly.graph_objects as go
import pandas as pd
import plotly.express as px
from controllers import preset_requests as Cpreset_requests
import dash_bootstrap_components as dbc

#MAIN CONTENT
def generate_dashboards(data):
    return create_heatmap_from_list(data_list=data)

def create_heatmap_from_list(data_list):
    """
    Crée une heatmap pour visualiser les agrégats par origine.
    """
    df = pd.DataFrame(data_list)  # Transformer la liste en DataFrame
    
    # Vérifier que le DataFrame contient les colonnes attendues
    assert "Origin" in df.columns, "La colonne 'Origin' est absente"
    assert "aggregat" in df.columns, "La colonne 'aggregat' est absente"
    
    # Créer la heatmap
    fig = px.density_heatmap(
        df,
        x="Origin",
        y="Origin",  # Exemple pour cet usage spécifique
        z="aggregat",
        color_continuous_scale="Viridis",
        title="Heatmap of Aggregates by Origin",
        template="plotly_white"
    )
    return fig