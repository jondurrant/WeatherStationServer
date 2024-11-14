from DashMetricGroup import  DashMetricGroup
import dash_daq as daq
from dash import Dash, html,  dash_table, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from MetricSample import MetricSample
from MetricsRainCumlative import MetricsRainCumlative 
import plotly.express as px
from math import  floor

class DashMetricGroupRain(DashMetricGroup):
    
    def setUnits(self, units):
        self.units = "mmps"
    
    def setRange(self, low, high):
        self.min = min(low, 0)
        self.max = max(high, 10)
        
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
        
        #Cumlative Metric
        self.cumlative = MetricsRainCumlative("RainCumlative", self.dbEng)
         
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
        guage = self.getGuage(title, samValue)
        
        #Summary card
        summary = self.getSummary()
        
        cumlSum = self.getCumlSummary()

        
        
        #group
        group = dbc.CardGroup([
            dbc.Col(dbc.Card(dbc.CardBody([guage])), width=2),
            dbc.Col(dbc.Card(dbc.CardBody([spark])), width=2), 
            dbc.Col(cumlSum, width=2),
            dbc.Col(summary, width=2)
            ]
        )
        return group
    
    
    
    def getSpark(self):
        hourly = self.cumlative.hourly(self.device, self.sensor, self.start, self.end)
        
        fig = px.bar(hourly, x='SampleTime', y='CumlativeMM')
        fig.update_yaxes(visible=False, showticklabels=False)
        fig.update_xaxes(visible=False, showticklabels=False)
        fig.update_layout(margin=dict(l=2, r=2, t=2, b=2))
        fig.update_layout({
            'plot_bgcolor': 'rgba(0, 0, 0, 0)',
            'paper_bgcolor': 'rgba(20, 0, 0, 0)',
            })
        return fig
        
           
    def getCumlSummary(self): 
        current = self.cumlative.current(self.device, self.sensor, self.end)
  
        samValue = 0
        samSince = 0
        samTime = "Never"
        if len(current) > 0:
            samValue = current['CumlativeMM'].values[0]
            samSince = current['SinceSec'].values[0]
            samTime = pd.Timestamp(current['SampleTime'].values[0]).strftime('%Y-%m-%d %X')
            
        m = floor(samSince / 60)
        h = floor(samSince / 60 / 60)
        s = samSince - (m * 60) - (h * 60 * 60)
        msg = "Not Raining"
        if (samSince == 0):
            msg = "RAINING"
        card = dbc.Card([
            dbc.CardHeader("Since Raining: %02d:%02d:%02d"%(h, m, s)),
            dbc.CardBody(
                [
                    html.H2(msg, className="card-title"),
                    html.H4("%dmm of Rain"%samValue),
                    html.P("%s on sensor %s. Last Updated: %s"%(self.metric, self.sensor, samTime), className="card-text"),
                ]
            )
        ],
        style=self.style,
        id=self.id + 'cumlSummary'
        )
        return card
        
        