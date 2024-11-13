import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData, desc, func, inspect
import sqlalchemy
import json
from MetricSample import MetricSample
from dash import Dash, html,  dash_table, dcc
import dash_bootstrap_components as dbc
import plotly.express as px
import dash_daq as daq

class DashMetricGroup:
    def __init__(self, device, metric, sensor, dbEng):
        self.device = device
        self.metric = metric
        self.sensor = sensor
        self.dbEng = dbEng
        self.setupStyle()
        self.id = "metric-%s-%s-"%(metric, sensor)
        
    def getGroup(self, title, ts = None):
        
        #Setup start and end date range
        self.end = ts
        if self.end == None:
            self.end = df.Timestamp.utcnow()
        self.start = self.end - pd.Timedelta(days=1) 
        
        #Sample Metric
        self.sample = MetricSample(self.metric, self.dbEng)
        current = self.sample.current(self.device, self.sensor, self.end)
        hourly = self.sample.hourly(self.device, self.sensor, self.start, self.end)
        
        #Spark
        spark =  dcc.Graph(
            figure=self.getSpark(),
            style=self.style,
            id=self.id + "spark"
            )
        
        samValue = 0
        samMax = 0
        samMin = 0
        if len(current) > 0:
            samValue = current['Sample'].values[0]
            samMax = current['Max'].values[0]
            samMin = current['Min'].values[0]
        
        #Guage
        self.guage = daq.Thermometer(
            label=title,
            labelPosition='top',
            value=samValue,
            width=10,
            scale={
                'start': 0, 
                'interval': 5,
                'labelInterval': 2,
                'style': {
                    'fontSize':40
                    }
            },
            min = -10,
            max = 35,
            style= self.style,
            color='red',
            units="C",
            id=self.id + "guage"
            )
        
        #Summary card
        self.summary = self.getSummary()
        
        #group
        group = dbc.CardGroup([
            dbc.Col(dbc.Card(dbc.CardBody([self.guage])), width=1),
            dbc.Col(dbc.Card(dbc.CardBody([spark])), width=2), 
            dbc.Col(self.summary)
            ]
        )
        return group
        
    def setupStyle(self):
        self.style = {
            "height": "250px"
        }
        
    def updateDevice(self, value):
        self.device = value
        
    def getCurrentSample(self):
        current = self.sample.current(self.device, self.sensor)
        samValue = 0
        if len(current) > 0:
            samValue = current['Sample'].values[0]
        return samValue
        
    def getSummary(self):
        current = self.sample.current(self.device, self.sensor, self.end)
  
        samValue = 0
        samMax = 0
        samMin = 0
        samTime = "Never"
        if len(current) > 0:
            samValue = current['Sample'].values[0]
            samMax = current['Max'].values[0]
            samMin = current['Min'].values[0]
            samTime = pd.Timestamp(current['SampleTime'].values[0]).strftime('%Y-%m-%d %X')
            
        self.summary = dbc.Card([
            dbc.CardHeader("Max: %.2fC"%(samMax)),
            dbc.CardBody(
                [
                    html.H4("%.2fC"%(samValue), className="card-title"),
                    html.P("Temperature AHT10. Last Updated: %s"%samTime, className="card-text"),
                ]
            ),
            dbc.CardFooter("Min: %.2fC"%(samMin)),
        ],
        style={"width": "18rem", "height": "250px"},
        id='metric-ETemp-aht10-summary'
        )
        return self.summary
    
    def getSpark(self):
        hourly = self.sample.hourly(self.device, self.sensor, self.start, self.end)
        
        fig = px.line(hourly, x='SampleTime', y='Sample')
        fig.update_yaxes(visible=False, showticklabels=False)
        fig.update_xaxes(visible=False, showticklabels=False)
        fig.update_layout(margin=dict(l=2, r=2, t=2, b=2))
        fig.update_layout({
            'plot_bgcolor': 'rgba(0, 0, 0, 0)',
            'paper_bgcolor': 'rgba(20, 0, 0, 0)',
            })
        return fig
       
    def updateDate(self, ts):
        self.end = pd.Timestamp(ts.year, ts.month, ts.day, 23, 59)
        self.start = self.end - pd.Timedelta(days=1)  
        