import dash
from dash import Dash, html, dcc, callback, Input, Output, State, MATCH
import dash_bootstrap_components as dbc

app = Dash(
    __name__, 
    use_pages=True, 
    external_stylesheets=[dbc.themes.CYBORG],
    suppress_callback_exceptions=True)

app.layout = html.Div([
    dcc.Store(id='session', storage_type='session', data={}),
    html.H1('Weather Station'),
    html.Div([
        html.Div(
            dcc.Link(f"{page['name']} - {page['path']}", href=page["relative_path"])
        ) for page in dash.page_registry.values()
    ]),
    dash.page_container
])


if __name__ == '__main__':
    

    
    app.run(debug=True)