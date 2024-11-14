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

from datetime import date, datetime


dbHost=os.environ.get("DB_HOST", "localhost")
dbPort=os.environ.get("DB_PORT", "3306")
dbSchema=os.environ.get("DB_SCHEMA", "root")
dbUser=os.environ.get("DB_USER", "root")
dbPasswd=os.environ.get("DB_PASSWD", "root")
connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
engine = create_engine(connectString)

devicesMgt = DevicesStatus(engine)
df = devicesMgt.getDataFrame()

layout = [
    html.Div(children='WeatherStations'),
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
        },
        style_table={'width': '80%'}
    )
]
    

dash.register_page(__name__)    
