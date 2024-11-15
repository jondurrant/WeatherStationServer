import dash
from dash import html, dcc, callback, Input, Output, ctx
from datetime import date, datetime
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import os

from DevicesStatus import DevicesStatus
from MetricSample import MetricSample
from MetricsRainCumlative import MetricsRainCumlative

dash.register_page(
    __name__,
    path='/analytics',
    title='Weather Station Analytics',
    name='Weather Station Analytics')  


dbHost=os.environ.get("DB_HOST", "localhost")
dbPort=os.environ.get("DB_PORT", "3306")
dbSchema=os.environ.get("DB_SCHEMA", "root")
dbUser=os.environ.get("DB_USER", "root")
dbPasswd=os.environ.get("DB_PASSWD", "root")
connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
global engine
engine = create_engine(connectString)



def layout(device, metric, end, **_kwargs):
    d = pd.Timestamp(end)
    endTS = pd.Timestamp(d.year, d.month, d.day, 23, 59, 59)
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


    #Device Selection
    devicesMgt = DevicesStatus(engine)
    devices = devicesMgt.getWeatherStations()
    deviceSel =  dcc.Dropdown(devices, device, id='deviceSel', style={"width": "200px"}),
    controlGrp = dbc.CardGroup([
        dbc.Col(dbc.Card(dbc.CardBody(deviceSel)), width=1),
        dbc.Col(dbc.Card(dbc.CardBody(html.H4("From"))), width=1),
        dbc.Col(dbc.Card(dbc.CardBody([startPic])), width=1),
        dbc.Col(dbc.Card(dbc.CardBody(html.H4("To"))), width=1),
        dbc.Col(dbc.Card(dbc.CardBody([endPic])), width=1),
        btnGrp
        ])
    
    layout = html.Div([
        html.H1('Weather Station Analytics'),
        controlGrp,
        dcc.Tabs(id="tabs-analytics", value='tab-temp', children=[
        dcc.Tab(label='Temperature', value='tab-temp'),
        dcc.Tab(label='Humidity', value='tab-humid'),
        dcc.Tab(label='Pressure', value='tab-pressure'),
        dcc.Tab(label='Wind', value='tab-wind'),
        dcc.Tab(label='Rain', value='tab-rain'),
        dcc.Tab(label='Light', value='tab-light'),
    ]),
    html.Div(id='tabs-content')
    ])
    return layout

def getTempFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("ETemp", engine)
    print("%s:%s"%(device, sensor))
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getHumidFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Humidity", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getPressureFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Pressure", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getLumiFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Lumi", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getUVFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("UV", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getAnemFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Anem", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getVainFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Vain", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.bar(hourly, x='SampleTime', y=['Sample'])
    return fig

def getRainFig(device, sensor, startTS, endTS):
    metricETemp = MetricSample("Rain", engine)
    hourly = metricETemp.hourly(device, sensor, startTS, endTS)
        
    fig = px.line(hourly, x='SampleTime', y=['Sample','Min','Max'])
    return fig

def getRainCumFig(device, sensor, startTS, endTS):
    metric = MetricsRainCumlative("RainCumlative", engine)
    hourly = metric.hourly(device, sensor, startTS, endTS)
        
    fig = px.bar(hourly, x='SampleTime', y=['mmPerHour'])
    return fig

def getTabs(tab, device, startTS, endTS):
    res = [html.H2("Unknown")]
    
    if (tab == "tab-temp"):
        #Temp
        fig1 = getTempFig(device, "aht10", startTS, endTS)
        temp1Graph = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-temp1"
                )
        fig2 = getTempFig(device, "sen0500", startTS, endTS)
        temp2Graph = dcc.Graph(
                figure=fig2,
                style={},
                id="analytics-temp2"
                )
        res = [
            html.H2("Temperature Detail"),
            html.H2("AHT10"),
            temp1Graph, 
            html.H2("SEN0500"),
            temp2Graph
            ]
    elif (tab == "tab-humid"):
        fig1 = getHumidFig(device, "aht10", startTS, endTS)
        graph1 = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-humid1"
                )
        fig2 = getHumidFig(device, "sen0500", startTS, endTS)
        graph2 = dcc.Graph(
                figure=fig2,
                style={},
                id="analytics-humid2"
                )
        res = [
            html.H2("Humidity Detail"),
            html.H2("AHT10"),
            graph1, 
            html.H2("SEN0500"),
            graph2
            ]
    elif tab == "tab-pressure":
        fig1 = getPressureFig(device, "sen0500", startTS, endTS)
        graph1 = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-pressure"
                )
        res = [
            html.H2("Atmospheric Pressure Detail"),
            html.H2("SEN0500"),
            graph1
            ]
    elif tab == "tab-light":
        fig1 = getLumiFig(device, "sen0500", startTS, endTS)
        graph1 = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-light1"
                )
        fig2 = getUVFig(device, "sen0500", startTS, endTS)
        graph2 = dcc.Graph(
                figure=fig2,
                style={},
                id="analytics-light2"
                )
        res = [
            html.H2("Light Levels Detail"),
            html.H2("Ambient Lx SEN0500"),
            graph1, 
            html.H2("UV SEN0500"),
            graph2
            ]
    elif tab == "tab-wind":
        fig1 = getAnemFig(device, "anem", startTS, endTS)
        graph1 = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-wind1"
                )
        fig2 = getVainFig(device, "vain", startTS, endTS)
        graph2 = dcc.Graph(
                figure=fig2,
                style={},
                id="analytics-wind2"
                )
        res = [
            html.H2("Wind Detail"),
            html.H2("Wind Strength"),
            graph1, 
            html.H2("Wind Direction"),
            graph2
            ]
    elif tab == "tab-rain":
        fig1 = getRainFig(device, "rain", startTS, endTS)
        graph1 = dcc.Graph(
                figure=fig1,
                style={},
                id="analytics-rain1"
                )
        fig2 = getRainCumFig(device, "rain", startTS, endTS)
        graph2 = dcc.Graph(
                figure=fig2,
                style={},
                id="analytics-rain2"
                )
        res = [
            html.H2("Rain Detail"),
            html.H2("Rain mmps"),
            graph1, 
            html.H2("Rain Cumlative"),
            graph2
            ]
  
    return res
    

@callback(
    Output('start-date', 'date'),
    Output('tabs-content', 'children'),
#    Output('analytics-temp1', 'figure'),
#    Output('analytics-temp2', 'figure'),
    Input('tabs-analytics', 'value'),
    Input('deviceSel', 'value'),
    Input('start-date', 'date'),
    Input('end-date', 'date'),
    Input('btn-day', 'n_clicks'),
    Input('btn-week', 'n_clicks'),
    Input('btn-month', 'n_clicks'),
    Input('btn-year', 'n_clicks')
)
def periodBtns(tab, device, startStr, endStr, btn1, btn2, btn3, btn4):
    d = pd.Timestamp(endStr)
    end = pd.Timestamp(d.year, d.month, d.day, 23, 59, 59)
    delta = pd.Timedelta(days=0) 
    d = pd.Timestamp(startStr)
    start = pd.Timestamp(d.year, d.month, d.day, 0, 0, 0)
    if "btn-day" == ctx.triggered_id:
        delta = pd.Timedelta(days=1)
        start = end - delta
    elif "btn-week" == ctx.triggered_id:
        delta = pd.Timedelta(days=7)
        start = end - delta
    elif "btn-month" == ctx.triggered_id:
        delta = pd.DateOffset(months=1)
        start = end - delta
    elif "btn-year" == ctx.triggered_id:
        delta = pd.DateOffset(years=1)
        start = end - delta
    
    return (
        start.strftime('%Y-%m-%d %X'),
        getTabs(tab, device, start, end)
        #getTempFig(device, "aht10", start, end),
        #getTempFig(device, "sen0500", start, end)
        )




