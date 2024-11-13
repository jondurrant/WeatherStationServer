from dash import Dash, html,  dash_table, dcc, Output, Input, callback, callback_context
import dash_daq as daq
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
from DevicesStatus import DevicesStatus
import os
from MetricSample import MetricSample
import plotly.express as px
import dash_bootstrap_components as dbc
from DashMetricGroup import  DashMetricGroup

from datetime import date, datetime


app = Dash(external_stylesheets=[dbc.themes.CYBORG])



@callback(
    Output('metric-ETemp-aht10-summary', 'children'),
    Input('device', 'value'),
    Input('date-picker', 'date')
)
def update_output(dev, dat):
    global tempGrp
    
    ts = pd.Timestamp(dat)
    tempGrp.updateDate(ts)
    tempGrp.updateDevice(dev)
    
    return tempGrp.getSummary()

@callback(
    Output('metric-ETemp-aht10-guage', 'value'),
    Input('device', 'value'),
    Input('date-picker', 'date')
)
def updateTempGauge(dev, dat):
    global tempGrp
    
    ts = pd.Timestamp(dat)
    tempGrp.updateDate(ts)
    tempGrp.updateDevice(dev)

    return tempGrp.getCurrentSample()

@callback(
    Output('metric-ETemp-aht10-spark', 'figure'),
    Input('device', 'value'),
    Input('date-picker', 'date')
)
def updateTempFigure(dev, dat):
    global tempGrp
   
    ts = pd.Timestamp(dat)
    tempGrp.updateDate(ts)
    tempGrp.updateDevice(dev)

    return tempGrp.getSpark()



if __name__ == '__main__':
    #setup DB connection 
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
    tempGrp = DashMetricGroup("E6614104032F3F39", "ETemp", "aht10", engine)
    
    datePic = dcc.DatePickerSingle(
        id='date-picker',
        min_date_allowed=date(2024, 1, 1),
        date=datetime.now()
    )
    
    app.layout = [
        html.Div(children='Device Selection', id='dd-output-container'),
        dcc.Dropdown(devices, devices[0], id='device', style={"width": "200px"}),
        datePic,
        tempGrp.getGroup("Temperature", end),
        html.Div(children='My First App with Data'),
        dash_table.DataTable(
            data=df.to_dict('records'), 
            page_size=10, 
        style_data={
            'color': 'black',
            'backgroundColor': 'white'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)',
            }
        ],
        style_header={
            'backgroundColor': 'rgb(210, 100, 100)',
            'color': 'black',
            'fontWeight': 'bold'
        })
    ]
    
    app.run(debug=True)