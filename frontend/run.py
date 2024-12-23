from dash import Input, Output, html, _dash_renderer
import dash_bootstrap_components as dbc
from app import app

#IMPORT PAGES
from pages.homepage import layout
import pages.homepage as homepage

_dash_renderer._set_react_version("18.2.0")

app.layout = layout


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    match pathname:
        case "/":
            return homepage.layout

if __name__ == '__main__':
    app.run(
        debug=True,
        threaded=True,
        host='127.0.0.1',
        port=8050,
    )