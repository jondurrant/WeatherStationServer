from DashMetricGroup import  DashMetricGroup
import dash_daq as daq
from dash import Dash, html,  dash_table, dcc
import dash_bootstrap_components as dbc
import pandas as pd
from MetricSample import MetricSample

class DashMetricGroupWind(DashMetricGroup):
    
    def setUnits(self, units):
        self.units = "kmph"
    
    def setRange(self, low, high):
        self.min = min(low, 0)
        self.max = max(high, 100)
        
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
        
        #Vain Metric
        self.vainSample = MetricSample("Vain", self.dbEng)
        
        
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
        
        #Vain
        vain = self.getVain()
        
        #group
        group = dbc.CardGroup([
            dbc.Col(dbc.Card(dbc.CardBody([guage])), width=2),
            dbc.Col(vain, width=2),
            dbc.Col(dbc.Card(dbc.CardBody([spark])), width=2), 
            dbc.Col(summary)
            ]
        )
        return group
    
    def getVain(self):
        data = self.vainSample.current(self.device, "vain", self.end)
  
        samValue = 0
        samMax = 0
        samMin = 0
        samTime = "Never"
        if len(data) > 0:
            samValue = data['Sample'].values[0]
            samMax = data['Max'].values[0]
            samMin = data['Min'].values[0]
            samTime = pd.Timestamp(data['SampleTime'].values[0]).strftime('%Y-%m-%d %X')
            
        self.vane = dbc.Card([
            dbc.CardHeader("Max: %.2f%s"%(samMax, "deg")),
            dbc.CardBody(
                [
                    html.H2(self.degToDesc(samValue), className="card-title"),
                    html.P("Wind direction. Last Updated: %s"%(samTime), className="card-text"),
                ]
            ),
            dbc.CardFooter("Min: %.2f%s"%(samMin, "deg")),
        ],
        style=self.style,
        id=self.id + 'vane'
        )
        return self.vane
    
    def degToDesc(self, deg):
        tab = [
            [0,"N"],
            [22.5, "NNE"],
            [45, "NE"],
            [67.5, "ENE"],
            [90,"E"],
            [112.5, "ESE"],
            [135, "SE"],
            [157.5, "SSE"],
            [180, "S"],
            [202.5, "SSW"],
            [225, "SW"],
            [247.5, "WSW"],
            [270, "W"],
            [292.5, "WNW"],
            [315, "NW"],
            [337.5, "NNW"]
            ]
        
        resDesc = "N"
        resAng = 0
        for dir in tab:
            if abs(resAng - deg) > abs(dir[0] - deg):
                resDesc = dir[1]
                resAng = dir[0]
        
        return resDesc
        
            
        