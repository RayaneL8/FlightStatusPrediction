import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import requests
import pandas as pd
import plotly.express as px

# URL de base de votre API FastAPI
BASE_URL = "http://127.0.0.1:8000"

# Initialisation de l'application Dash avec un thème Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Mise en page de l'application
app.layout = dbc.Container([
    html.H1("Tableau interactif des données de vols", className="my-4"),

    # Boutons pour lancer différentes requêtes
    dbc.ButtonGroup([
        dbc.Button("Liste des compagnies", id="btn-list-airlines", color="primary", className="me-2"),
        dbc.Button("Calendrier des vols annulés", id="btn-cancelled-flights-calendar", color="danger")
    ], className="mb-3"),

    # Div pour afficher le tableau ou le graphique
    html.Div(id="output-content", className="mt-3")
])

# Callback pour gérer les boutons
@app.callback(
    Output("output-content", "children"),
    [Input("btn-list-airlines", "n_clicks"),
     Input("btn-cancelled-flights-calendar", "n_clicks")],
    prevent_initial_call=True
)
def update_content(n1, n2):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update

    # Identifier le bouton déclencheur
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    if button_id == "btn-list-airlines":
        try:
            # Requête pour récupérer la liste des compagnies
            response = requests.get(f"{BASE_URL}/list-airlines")
            response.raise_for_status()
            data = response.json()

            # Convertir en DataFrame
            df = pd.DataFrame(data)

            # Générer un tableau HTML
            header = html.Thead(html.Tr([html.Th(col) for col in df.columns]))
            rows = [
                html.Tr([html.Td(df.iloc[i][col]) for col in df.columns])
                for i in range(len(df))
            ]
            body = html.Tbody(rows)
            return dbc.Table([header, body], striped=True, bordered=True, hover=True)

        except Exception as e:
            return html.Div(f"Erreur : {e}")

    elif button_id == "btn-cancelled-flights-calendar":
        try:
            # Requête pour récupérer les données du calendrier
            response = requests.get(f"{BASE_URL}/cancelled-flights-calendar")
            response.raise_for_status()
            data = response.json()

            # Convertir en DataFrame
            df = pd.DataFrame(data)
            df['Date'] = pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'].astype(str) + '-01')

            # Forcer l'axe des mois à être catégoriel (1 à 12)
            df['Month'] = df['Month'].astype(str)  # Convertir les mois en chaînes pour les traiter comme catégories

            # Créer un graphique de calendrier
            fig = px.density_heatmap(
                df,
                x="Month",
                y="Year",
                z="CancelledPercentage",
                text_auto=".2f",
                color_continuous_scale="Reds",
                labels={"CancelledPercentage": "Pourcentage annulé", "Month": "Mois", "Year": "Année"},
                title="Pourcentage de vols annulés par mois",
            )

            fig.update_layout(
                xaxis_title="Mois",
                yaxis_title="Année",
                xaxis=dict(
                    tickmode='array',
                    tickvals=[str(i) for i in range(1, 13)],  # Afficher les mois de 1 à 12
                    ticktext=[str(i) for i in range(1, 13)]
                ),
                template="plotly_white"
            )

            return dcc.Graph(figure=fig)

        except Exception as e:
            return html.Div(f"Erreur : {e}")

    return dash.no_update


# Lancement de l'application
if __name__ == "__main__":
    app.run_server(debug=True)
