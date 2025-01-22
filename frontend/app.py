from dash import Dash
import dash_bootstrap_components as dbc

app = Dash(
    title="Flight Catcher",
    external_stylesheets=["bootstrap.min.css", "bootstrap.css", dbc.themes.BOOTSTRAP, "https://fonts.googleapis.com/css2?family=Cinzel:wght@400;700&display=swap"],
    prevent_initial_callbacks="initial_duplicate",
    suppress_callback_exceptions=True
)