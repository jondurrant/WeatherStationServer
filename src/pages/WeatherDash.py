from dash import Dash, html,  dash_table, dcc, Output, Input, callback, callback_context, MATCH
import dash
import dash_daq as daq
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
from DevicesStatus import DevicesStatus
import os
from MetricSample import MetricSample
import plotly.express as px
import dash_bootstrap_components as dbc
from DashMetricGroup import  DashMetricGroup
from DashMetricGroupTemp import  DashMetricGroupTemp
from DashMetricGroupWind import  DashMetricGroupWind
from DashMetricGroupRain import  DashMetricGroupRain
from datetime import date, datetime



@callback(
    Output('session', 'data', allow_duplicate=True),
    Output('metric-ETemp-aht10-summary', 'children'),
    Output('metric-ETemp-aht10-guage', 'value'),
    Output('metric-ETemp-aht10-spark', 'figure'),
    Output('metric-Humidity-aht10-summary', 'children'),
    Output('metric-Humidity-aht10-guage', 'value'),
    Output('metric-Humidity-aht10-spark', 'figure'),
    Output('metric-Pressure-sen0500-summary', 'children'),
    Output('metric-Pressure-sen0500-guage', 'value'),
    Output('metric-Pressure-sen0500-spark', 'figure'),
    Output('metric-Anem-anem-summary', 'children'),
    Output('metric-Anem-anem-vane', 'children'),
    Output('metric-Anem-anem-guage', 'value'),
    Output('metric-Anem-anem-spark', 'figure'),
    Output('metric-Rain-rain-summary', 'children'),
    Output('metric-Rain-rain-cumlSummary', 'children'),
    Output('metric-Rain-rain-guage', 'value'),
    Output('metric-Rain-rain-spark', 'figure'),
    Output('metric-Lumi-sen0500-summary', 'children'),
    Output('metric-Lumi-sen0500-guage', 'value'),
    Output('metric-Lumi-sen0500-spark', 'figure'),
    Output('metric-UV-sen0500-summary', 'children'),
    Output('metric-UV-sen0500-guage', 'value'),
    Output('metric-UV-sen0500-spark', 'figure'),
    Input('deviceSel', 'value'),
    Input('date-picker', 'date'),
    prevent_initial_call=True
)
def updateTempSummary(dev, dat):
    global tempGrp
    global humidGrp
    global pressureGrp
    global windGrp
    global rainGrp
    global lumiGrp
    global uvGrp
    
    ts = pd.Timestamp(dat)
    end = pd.Timestamp(ts.year, ts.month, ts.day, 23, 59, 59)
   
    return (
        {"device": dev, "end": end},
        tempGrp.getSummary(dev, end),
        tempGrp.getCurrentSample(dev, end),
        tempGrp.getSpark(dev, end),
        
        humidGrp.getSummary(dev, end),
        humidGrp.getCurrentSample(dev, end),
        humidGrp.getSpark(dev, end),
        
        pressureGrp.getSummary(dev, end),
        pressureGrp.getCurrentSample(dev, end),
        pressureGrp.getSpark(dev, end),
        
        windGrp.getSummary(dev, end),
        windGrp.getVain(dev, end),
        windGrp.getCurrentSample(dev, end),
        windGrp.getSpark(dev, end),
        
        rainGrp.getSummary(dev, end),
        rainGrp.getCumlSummary(dev, end),
        rainGrp.getCurrentSample(dev, end),
        rainGrp.getSpark(dev, end),
        
        lumiGrp.getSummary(dev, end),
        lumiGrp.getCurrentSample(dev, end),
        lumiGrp.getSpark(dev, end),
        
        uvGrp.getSummary(dev, end),
        uvGrp.getCurrentSample(dev, end),
        uvGrp.getSpark(dev, end)
        )



dbHost=os.environ.get("DB_HOST", "localhost")
dbPort=os.environ.get("DB_PORT", "3306")
dbSchema=os.environ.get("DB_SCHEMA", "root")
dbUser=os.environ.get("DB_USER", "root")
dbPasswd=os.environ.get("DB_PASSWD", "root")
connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
engine = create_engine(connectString)


devicesMgt = DevicesStatus(engine)
df = devicesMgt.getDataFrame()
devices = devicesMgt.getWeatherStations()



end = pd.Timestamp(2024, 11, 10, 16, 00, 00)
start = end - pd.Timedelta(days=1) 



global tempGrp
tempGrp = DashMetricGroupTemp(
    "ETemp", 
    "aht10", 
    engine)
global humidGrp
humidGrp = DashMetricGroup(
    "Humidity", 
    "aht10", 
    engine, 
    units="%",
    low=10,
    high=100)

global pressureGrp
pressureGrp = DashMetricGroup(
    "Pressure", 
    "sen0500", 
    engine, 
    units="hPa",
    low=700,
    high=1200)

global windGrp
windGrp = DashMetricGroupWind( 
    "Anem", 
    "anem", 
    engine
    )

global rainGrp
rainGrp = DashMetricGroupRain(
    "Rain", 
    "rain", 
    engine
    )

global lumiGrp
lumiGrp = DashMetricGroup(
    "Lumi", 
    "sen0500", 
    engine, 
    units="lx",
    low=0,
    high=25000)

global uvGrp
uvGrp = DashMetricGroup( 
    "UV", 
    "sen0500", 
    engine, 
    units="mW/m2",
    low=0,
    high=200)

datePic = dcc.DatePickerSingle(
    id='date-picker',
    min_date_allowed=date(2024, 1, 1),
    date=datetime.now()
)

#Device Selection
deviceSel =  dcc.Dropdown(devices, devices[0], id='deviceSel', style={"width": "200px"}),

controlGrp = dbc.CardGroup([
    dbc.Col(dbc.Card(dbc.CardBody(html.H4("Device"))), width=1),
    dbc.Col(dbc.Card(dbc.CardBody(deviceSel)), width=1),
    dbc.Col(dbc.Card(dbc.CardBody(html.H4("Date"))), width=1),
    dbc.Col(dbc.Card(dbc.CardBody([datePic])), width=1),
    ])

layout = [
    html.Div(children='Device Selection', id='dd-output-container'),
    controlGrp,
    tempGrp.getGroup("Temperature", end),
    humidGrp.getGroup("Humidity", end),
    pressureGrp.getGroup("Pressure", end),
    windGrp.getGroup("Wind", end),
    rainGrp.getGroup("Rain", end),
    lumiGrp.getGroup("Ambient Light", end),
    uvGrp.getGroup("UV", end)
]
    

dash.register_page(
    __name__,
    path='/weather-dashboard',
    title='Weather Dashboard',
    name='Weather Station Dashboard')    
