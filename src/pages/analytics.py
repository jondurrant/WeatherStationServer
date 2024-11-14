import dash
from dash import html, dcc, callback, Input, Output, ctx
from datetime import date, datetime
import dash_bootstrap_components as dbc
import pandas as pd

dash.register_page(
    __name__,
    path='/analytics',
    title='Weather Station Analytics',
    name='Weather Station Analytics')  





def layout(metric, end, **_kwargs):
    endTS = pd.Timestamp(end)
    delta = pd.Timedelta(days=1) 
    startTS = endTS - delta
    
    print(end)
    print(endTS)
    print(startTS)
    
    startPic = dcc.DatePickerSingle(
        id='start-date',
        min_date_allowed=date(2024, 1, 1),
        date=startTS
    )
    endPic = dcc.DatePickerSingle(
        id='end-date',
        min_date_allowed=date(2024, 1, 1),
        date=endTS
    )
    
    btnGrp = dbc.CardGroup([
        dbc.Col(dbc.Card(dbc.CardBody(
            html.Button('Day', id='btn-day', n_clicks=0)
            ))),
        dbc.Col(dbc.Card(dbc.CardBody(
            html.Button('Week', id='btn-week', n_clicks=0)
            ))),
        dbc.Col(dbc.Card(dbc.CardBody(
            html.Button('Month', id='btn-month', n_clicks=0)
            ))),
        dbc.Col(dbc.Card(dbc.CardBody(
            html.Button('Year', id='btn-year', n_clicks=0)
            )))
        ])

    controlGrp = dbc.CardGroup([
        dbc.Col(dbc.Card(dbc.CardBody(html.H4("From"))), width=1),
        dbc.Col(dbc.Card(dbc.CardBody([startPic])), width=1),
        dbc.Col(dbc.Card(dbc.CardBody(html.H4("To"))), width=1),
        dbc.Col(dbc.Card(dbc.CardBody([endPic])), width=1),
        btnGrp
        ])
    
    #Temp
    temp = dcc.Graph(
            figure=self.getSpark(),
            style=self.style,
            id=self.id + "temp"
            )
    
    layout = html.Div([
        html.H1('Weather Station Analytics'),
        controlGrp
    ])
    return layout

@callback(
    Output('start-date', 'date'),
    Input('end-date', 'date'),
    Input('btn-day', 'n_clicks'),
    Input('btn-week', 'n_clicks'),
    Input('btn-month', 'n_clicks'),
    Input('btn-year', 'n_clicks')
)
def periodBtns(endStr, btn1, btn2, btn3, btn4):
    end = pd.Timestamp(endStr)
    delta = pd.Timedelta(days=0) 
    if "btn-day" == ctx.triggered_id:
        delta = pd.Timedelta(days=1)
    elif "btn-week" == ctx.triggered_id:
        delta = pd.Timedelta(days=7)
    elif "btn-month" == ctx.triggered_id:
        delta = pd.DateOffset(months=1)
    elif "btn-year" == ctx.triggered_id:
        delta = pd.DateOffset(years=1)
    
    start = end - delta
    return start.strftime('%Y-%m-%d %X')




