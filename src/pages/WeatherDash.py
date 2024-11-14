from dash import Dash, html,  dash_table, dcc, Output, Input, callback, callback_context
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
    Input('device', 'value'),
    Input('date-picker', 'date')
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
    tempGrp.updateDate(ts)
    tempGrp.updateDevice(dev)
    humidGrp.updateDate(ts)
    humidGrp.updateDevice(dev)
    pressureGrp.updateDate(ts)
    pressureGrp.updateDevice(dev)
    windGrp.updateDate(ts)
    windGrp.updateDevice(dev)
    rainGrp.updateDate(ts)
    rainGrp.updateDevice(dev)
    lumiGrp.updateDate(ts)
    lumiGrp.updateDevice(dev)
    uvGrp.updateDate(ts)
    uvGrp.updateDevice(dev)
    
        
    return (
        tempGrp.getSummary(),
        tempGrp.getCurrentSample(),
        tempGrp.getSpark(),
        
        humidGrp.getSummary(),
        humidGrp.getCurrentSample(),
        humidGrp.getSpark(),
        
        pressureGrp.getSummary(),
        pressureGrp.getCurrentSample(),
        pressureGrp.getSpark(),
        
        windGrp.getSummary(),
        windGrp.getVain(),
        windGrp.getCurrentSample(),
        windGrp.getSpark(),
        
        rainGrp.getSummary(),
        rainGrp.getCumlSummary(),
        rainGrp.getCurrentSample(),
        rainGrp.getSpark(),
        
        lumiGrp.getSummary(),
        lumiGrp.getCurrentSample(),
        lumiGrp.getSpark(),
        
        uvGrp.getSummary(),
        uvGrp.getCurrentSample(),
        uvGrp.getSpark()
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
tempGrp = DashMetricGroupTemp(devices[0], "ETemp", "aht10", engine)
global humidGrp
humidGrp = DashMetricGroup(
    devices[0], 
    "Humidity", 
    "aht10", 
    engine, 
    units="%",
    low=10,
    high=100)

global pressureGrp
pressureGrp = DashMetricGroup(
    devices[0], 
    "Pressure", 
    "sen0500", 
    engine, 
    units="hPa",
    low=700,
    high=1200)

global windGrp
windGrp = DashMetricGroupWind(
    devices[0], 
    "Anem", 
    "anem", 
    engine
    )

global rainGrp
rainGrp = DashMetricGroupRain(
    devices[0], 
    "Rain", 
    "rain", 
    engine
    )

global lumiGrp
lumiGrp = DashMetricGroup(
    devices[0], 
    "Lumi", 
    "sen0500", 
    engine, 
    units="lx",
    low=0,
    high=25000)

global uvGrp
uvGrp = DashMetricGroup(
    devices[0], 
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

layout = [
    html.Div(children='Device Selection', id='dd-output-container'),
    dcc.Dropdown(devices, devices[0], id='device', style={"width": "200px"}),
    datePic,
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
