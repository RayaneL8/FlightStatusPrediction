from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
from pandas import DataFrame
import plotly.graph_objects
from app import app

import dash_mantine_components as dmc
from enum import Enum

import plotly.express as px

import numpy as np
import random

from settings import Settings

settings = Settings()

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "right": 0,
    "width": "100%",
    "height": "6rem",
    "padding": "2rem 1rem",
    "background": "linear-gradient(90deg, rgba(8,2,110,1) 0%, rgba(0,43,143,1) 12%, rgba(57,162,245,1) 79%)",
}

# the styles for the main content position it to the right of the sidebar and
# add some padding.
CONTENT_STYLE = {
    "margin-top": "4rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}


content = html.Div(
    id="page-content",
    style=CONTENT_STYLE

)

layout = html.Div(
    [
        dcc.Location(id="location-url"),
        html.Div(html.H1("Flight Catcher", style={"fontSize": "32px", "textAlign": "right"}), style=SIDEBAR_STYLE),
        content
    ]
)