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
from dataclasses import dataclass, asdict


#CLASSES AND DEFINITIONS ---------------------------------

@dataclass
class ReqHistoried:
    index: int
    raw_text: str
    sql_query: str
    valid: bool
    result: dict | None

#CONTENT_LAYOUT ------------------------------------------

def content_layout():
    return dmc.MantineProvider(
        content_template()
    )

#INTERFACE -----------------------------------------------



stores= [
    dcc.Store(id="store-history", data=[]),
    #dcc.Store(id="store-last-request", data = None), 
]

def content_template():
    return dbc.Container(
        children=[
            *stores,
            dbc.Offcanvas(
                dbc.Accordion(
                    id="accordion-history",
                    children=[],
                    flush=True
                ),
                id="offcanvas-history",
                title="Requests History",
                is_open=False,
                style={"width": "40%"}
            ),
            dbc.Row(
                children=[
                    dbc.Col(
                        dbc.Button(
                            id="button-open-history",
                            children= [
                                html.Img(
                                    src="../assets/history_icon.svg",
                                    style={"height": "64px"}
                                )
                            ]
                        ),
                        width=2
                    ),
                    dbc.Col(
                        children=[
                            dbc.Textarea(
                                id="textarea-request",
                                size="lg",
                                className="mb-3 w-100",
                                placeholder="put a request... (ex: \"i want to know which company have the highest average delay in 2020\")",
                                style={}
                            )
                        ],
                        width=8
                    ),
                    dbc.Col(
                        dbc.Button(
                            id="button-submit-request",
                            children= [
                                html.Img(
                                    src="../assets/rocket.gif",
                                    style={"height": "64px"}
                                )
                            ]
                        ),
                        width=2
                    ),
                ]
            ),
            dbc.Row(
                justify="center",  # Centre les boutons horizontalement
                children=[
                    dbc.Col(
                        children=[
                            dbc.Button(
                                "Performance", id="button-performance",
                                className="mx-2"  # Ajoute un espacement horizontal
                            )
                        ],
                        width="auto"
                    ),
                    dbc.Col(
                        children=[
                            dbc.Button(
                                "Statistics", id="button-statistics",
                                className="mx-2"  # Ajoute un espacement horizontal
                            )
                        ],
                        width="auto"
                    ),
                    dbc.Col(
                        children=[
                            dbc.Button(
                                "Quality", id="button-quality",
                                className="mx-2"  # Ajoute un espacement horizontal
                            )
                        ],
                        width="auto"
                    )
                ]
            )
        ],
        fluid=True, style={"padding": "32px", "textAlign": "center", "width": "100%"}
    )



def generate_accordion_item_history(req: ReqHistoried):
    def trim_string(s: str, length: int = 10, suffix: str = "...") -> str:
        return s[:length] + suffix if len(s) > length else s
    
    print("In generate_accordion_item_history()", req)
    accordion_item_style ={
        "whiteSpace": "normal",  # Permet le retour à la ligne
        "wordBreak": "break-word",  # Coupe les mots trop longs
    }
    item = dbc.AccordionItem(
        children=[
            html.P(req["raw_text"], style=accordion_item_style),
            html.P(req["sql_query"], style= accordion_item_style),
            html.P(req["valid"])
        ],
        title=f"Req°{req["index"]} | {trim_string(s= req["raw_text"], length= 40)}"
    )
    return item

#CALLBACKS -------------------------
@app.callback(
    Output("offcanvas-history", "is_open"),
    Input("button-open-history", "n_clicks"),
    State("offcanvas-history", "is_open")
)
def toggle_offcanvas(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open

@app.callback(
    Output("store-history", "data"),
    Input("button-submit-request", "n_clicks"),
    State("textarea-request", "value"),
    State("store-history", "data")
)
def submit_request(n_clicks: int, value: str, data: list):
    if not n_clicks:
        return data
    #Une fois l'API connectée, utiliser un ASYNC pour créer un objet ReqHistoried et l'ajouter à "data" en remplissant ses champs
    #Version de test en attendant 
    data.append(asdict(ReqHistoried(index=len(data), raw_text=value, sql_query="NONE", valid=True, result={})))
    print("In Submit_Request(): ", data)
    return data

@app.callback(
    Output("accordion-history", "children"),
    Input("store-history", "data"),
    State("accordion-history", "children")
)
def fill_accordion_history(data: list, children: list):
    if len(data) < 1: 
        return []
    new_req = data[len(data)-1]
    print("In fill_accordion_history()", new_req)
    children.insert(0, generate_accordion_item_history(new_req))
    return children
