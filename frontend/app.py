from dash import Dash
import dash_bootstrap_components as dbc

app = Dash(
    title="Flight Catcher",
    external_stylesheets=["bootstrap.min.css", "bootstrap.css"],
    prevent_initial_callbacks="initial_duplicate",
    suppress_callback_exceptions=True
)