from datetime import date, datetime
from dash import html, dcc, Input, Output, State, ctx, ALL
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
from dataclasses import asdict
from models.Requests import ReqHistoried
from controllers import natural_requests as Cnat_reqs
from controllers import initiators as Cinitiators
from controllers import preset_requests as Cpreset_requests
from components import performance_dashboards, statistics_dashboard, quality_dashboard

import dash_mantine_components as dmc
from dash_iconify import DashIconify

#CLASSES AND DEFINITIONS ---------------------------------



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
            dbc.Modal(
                [
                    dbc.ModalHeader(dbc.ModalTitle("Request's parameters selector"), close_button=True),
                    dbc.ModalBody(
                        children= [
                            dbc.Col(
                                [
                                    html.Div(
                                        "Cities selected:",
                                        style={"marginBottom": "12px"},
                                    ),
                                    dcc.Dropdown(
                                        id="dropdown-cities",
                                        multi=True,
                                        options=Cinitiators.get_cities(),
                                        value=[],
                                        style={"marginBottom": "16px", "marginTop": "8px"}
                                    ),
                                ]
                            ),
                            dbc.Col(
                                [
                                    html.Div(
                                        "Airlines selected:",
                                        style={"marginBottom": "12px"},
                                    ),
                                    dcc.Dropdown(
                                        id="dropdown-airlines",
                                        multi=True,
                                        options=Cinitiators.get_airlines(),
                                        value=[],
                                        style={"marginBottom": "16px", "marginTop": "8px"}
                                    ),
                                ]
                            ),
                            dbc.Col(
                                [
                                    html.Div(
                                        "Year selected (only year will matter in your selection):",
                                        style={"marginBottom": "12px"},
                                    ),
                                    # dcc.DatePickerSingle(
                                    #     id="datepicker",
                                    #     month_format='MMM Do, YY',
                                    #     placeholder='MMM Do, YY',
                                    #     date=date(2018, 1, 1),
                                    #     min_date_allowed=date(2018, 1, 1),
                                    #     max_date_allowed=date(2022, 12, 31),
                                    #     style={"marginBottom": "16px", "marginTop": "8px"}
                                    # ),
                                ]
                            ),
                            dbc.Row(
                                [
                                    html.Div(
                                        "Choose type:",
                                        style={"marginBottom": "16px"}
                                    ),
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
                                ],
                                justify="center",
                                style={"padding": "16px", "textAlign": "center"}
                            )
                        ]
                    ),
                    dbc.ModalFooter(
                        dbc.Button(
                            "Submit",
                            id="button-submit-modal-parameters-selection",
                            className="ms-auto",
                            n_clicks=0,
                        )
                    ),
                ],
                id="modal-parameters-selection",
                centered=True,
                is_open=False,
                size="xl"
            ),
            dbc.Modal(
                children=[dbc.Button("Close", id="button-close-modal-display-raw-data", className="ms-auto", n_clicks=0)],
                id="modal-display-raw-data",
                is_open=False,
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
                        dbc.Button("Request", id="button-open-modal-parameters-selection"),
                        width="auto"
                    ),
                ],
            ),
            dbc.Container(
                id="container-dashboards",
                children= [

                ],
            )
        ],
        fluid=True, style={"padding": "32px", "textAlign": "center", "width": "100%"}
    )



def generate_accordion_item_history(req: ReqHistoried):
    def trim_string(s: str, length: int = 10, suffix: str = "...") -> str:
        return s[:length] + suffix if len(s) > length else s
    
    # print("In generate_accordion_item_history()", req)
    accordion_item_style ={
        "whiteSpace": "normal",  # Permet le retour à la ligne
        "wordBreak": "break-word",  # Coupe les mots trop longs
    }
    icon_size = "48px"
    item = dbc.AccordionItem(
        style={"display": "flex"},
        children=[
            html.P(req["raw_text"], style=accordion_item_style),
            html.P(req["sql_query"], style= accordion_item_style),
            html.P(f"Duration (client): {req["elapsed_time"]["client"]}"),
            html.P(f"Duration (server): {req["elapsed_time"]["server"]}"),
            dbc.Button("Open data", id={"type": "button-display-data", "index": req["index"]})
            #html.P(html.Img(src="../assets/receipt.gif", style={"width": icon_size,"height": icon_size}) if req["valid"] else html.Img(src="../assets/error.gif", style={"height": icon_size, "width": icon_size}))
        ],
        title=html.Span(children=[f"Req°{req["index"]} | {trim_string(s= req["raw_text"], length= 40)} | ", html.Img(src="../assets/receipt.gif", style={"width": icon_size,"height": icon_size, "justifyContent": "flex-end"}) if req["valid"] else html.Img(src="../assets/error.gif", style={"height": icon_size, "width": icon_size})])
    )
    return item

#CALLBACKS -------------------------
@app.callback(
    Output("modal-display-raw-data", "children"),
    Output("modal-display-raw-data", "is_open"),
    Input({"type": "button-display-data", "index": ALL}, "n_clicks"),
    Input("button-close-modal-display-raw-data", "n_clicks"),
    State("modal-display-raw-data", "is_open"),
    State("store-history", "data"),
    prevent_initial_call=True
)
def toggle_display_data(n_open, n_close, is_open, data):
    if (n_open or n_close) and is_open:
        return [], not is_open

    from controllers.natural_requests import find_item, format_item_data
    # Trouver quel bouton a été cliqué
    triggered_index = ctx.triggered_id["index"]
    return [
        dbc.ModalHeader(dbc.ModalTitle("Display RAW Data of Request")),
        dbc.ModalBody(format_item_data(item=find_item(triggered_index))),
        dbc.ModalFooter(
            dbc.Button(
                "Close", id="button-close-modal-display-raw-data", className="ms-auto", n_clicks=0
            )
        )
    ], not is_open


@app.callback(
       Output("modal-parameters-selection", "is_open"),
       Input("button-open-modal-parameters-selection", "n_clicks"),
       State("modal-parameters-selection", "is_open")
)
def open_modal(n1, is_open):
    if n1 :
        return True
    return is_open

@app.callback(
    Output("container-dashboards", "children"),
    Input("button-submit-modal-parameters-selection", "n_clicks"),
    Input("button-performance", "n_clicks"),
    Input("button-statistics", "n_clicks"),
    Input("button-quality", "n_clicks"),
    State("dropdown-cities", "value"),
    State("dropdown-airlines", "value"),
    #State("datepicker", "date"),
    prevent_initial_call=True
)
def generate_dashboards(n_clicks_submit, n_clicks_perf, n_clicks_stat, n_clicks_qual, cities, airlines):       #year=2020
    triggered_id = ctx.triggered_id  # ctx permet de savoir quel élément a déclenché la callback
    
    if triggered_id == "button-statistics":
        data = Cpreset_requests.get_statistics(cities=cities, airlines=airlines, years=[2018,2019,2020,2021,2022])
        return statistics_dashboard.generate_dashboards(data=data)

    elif triggered_id == "button-performance":
        # date_str = year
        # year = int(datetime.strptime(date_str, "%Y-%m-%d").year)
        all_metrics, us_map_metrics = Cpreset_requests.get_performance(cities=cities, airlines=airlines, years=[2018,2019,2020,2021,2022])
        # print("all_metrics: ", all_metrics)
        return performance_dashboards.generate_dashboards(all_metrics=all_metrics, us_map_metrics=us_map_metrics, cities=cities, airlines=airlines)

    # Optionnel : Gérer d'autres cas (ex: qualité)
    elif triggered_id == "button-quality":
        data = Cpreset_requests.get_quality(cities=cities, airlines=airlines, years=[2018,2019,2020,2021,2022])
        return quality_dashboard.generate_dashboards(data=data)

    return "Aucun bouton pertinent cliqué"

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
    State("store-history", "data"),
    #prevent_initial_call=True
)
def submit_request(n_clicks: int, value: str, data: list):
    print("Dans le callback")

    if data is None:
        data = []

    if not n_clicks:
        return data

    # Initialiser la liste si nécessaire
    

    # Vérifier si le champ de texte est vide
    if not value or value.strip() == "":
        print("Texte de la requête vide, aucune action effectuée.")
        return data

    try:
        print("CLICK")
        # Appeler la fonction asynchrone pour traiter la soumission
        new_item = Cnat_reqs.handle_submit(content=value)
        # print("Nouvel élément ajouté :", new_item)

        # Récupérer la liste mise à jour
        updated_list = Cnat_reqs.get_list()  # Doit retourner une liste JSON-sérialisable
        # print("Liste mise à jour :", updated_list)

        # Retourner la liste mise à jour
        return updated_list
    except Exception as e:
        print(f"Erreur lors du traitement de la requête : {e}")
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
    # print("In fill_accordion_history()", new_req)
    children.insert(0, generate_accordion_item_history(new_req))
    return children