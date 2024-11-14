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
    def __init__(self, device, metric, sensor, dbEng, units="", low=0, high=10):
        self.device = device
        self.metric = metric
        self.sensor = sensor
        self.dbEng = dbEng
        self.setupStyle()
        self.id = "metric-%s-%s-"%(metric, sensor)
        self.setUnits(units)
        self.setRange(low, high)
        self.cache={}
        
    def setUnits(self, units):
        self.units = units
        
    def setRange(self, low, high):
        self.min = low
        self.max = high
        
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
        self.guage = self.getGuage(title, samValue)
        
        #Summary card
        self.summary = self.getSummary()
        
        #group
        group = dbc.CardGroup([
            dbc.Col(dbc.Card(dbc.CardBody([self.guage])), width=2),
            dbc.Col(dbc.Card(dbc.CardBody([spark])), width=2), 
            dbc.Col(self.summary, width=2)
            ]
        )
        return group
        
    def setupStyle(self):
        self.style = {
            "height": "250px",
            "width": "18rem"
        }
        
    def updateDevice(self, value):
        if self.device != value:
            self.device = value
            self.cache={}
        
    def getCurrentSample(self):
        current = self.cache.get(
            "current",
            self.sample.current(self.device, self.sensor, self.end))
        samValue = 0
        if len(current) > 0:
            samValue = current['Sample'].values[0]
        return samValue
        
    def getSummary(self):
        current = self.cache.get(
            "current",
            self.sample.current(self.device, self.sensor, self.end))
  
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
            dbc.CardHeader("Max: %.2f%s"%(samMax, self.units)),
            dbc.CardBody(
                [
                    html.H4("%.2f%s"%(samValue, self.units), className="card-title"),
                    html.P("%s on sensor %s. Last Updated: %s"%(self.metric, self.sensor, samTime), className="card-text"),
                ]
            ),
            dbc.CardFooter("Min: %.2f%s"%(samMin, self.units)),
        ],
        style=self.style,
        id=self.id + 'summary'
        )
        return self.summary
    
    def getSpark(self):
        hourly = self.cache.get(
            "hourly",
            self.sample.hourly(self.device, self.sensor, self.start, self.end)
            )
        
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
        end = pd.Timestamp(ts.year, ts.month, ts.day, 23, 59)
        self.end = end
        self.start = self.end - pd.Timedelta(days=1)  
        self.cache={}
  
        
    def getGuage(self, title, value = 0):
        return daq.Gauge(
            showCurrentValue=True,
            value=value,
            label=title,
            units=self.units,
            min = self.min,
            max = self.max,
            id=self.id + "guage",
            style= self.style,
            color={"gradient":True,"ranges":{
                "green":[self.min,((self.max - self.min)/3) + self.min],
                "yellow":[
                    (self.max - self.min)/3 + self.min, (self.max - self.min)/3 *2 +self.min],
                "red":[(self.max - self.min)/3 *2 + self.min, self.max]}}
        )
    
        