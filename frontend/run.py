from dash import dcc, Input, Output, html, _dash_renderer
import dash_bootstrap_components as dbc
from app import app

#IMPORT PAGES
from pages.layout import layout
from pages import home

_dash_renderer._set_react_version("18.2.0")

app.layout = layout


@app.callback(Output("page-content", "children"), [Input("location-url", "pathname")])
def render_page_content(pathname):
    match pathname:
        case "/":
            return home.content_layout()
        
        case _:
            dbc.Jumbotron(
                [
                    html.H1("404: Not found", className="text-danger"),
                    html.Hr(),
                    html.P(f"Pathname unrecognized, make sure to use a valid url")
                ]
            )

if __name__ == '__main__':
    app.run(
        debug=True,
        threaded=True,
        host='127.0.0.1',
        port=8050,
    )