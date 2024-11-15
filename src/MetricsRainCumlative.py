import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData, desc, func, inspect
import sqlalchemy
import json

from MetricSample import MetricSample

def _default_get_rain_cumlative(dic):
    return dic.get("rain", None)

class MetricsRainCumlative(MetricSample):
    def buildTables(self):
        self.metadata=MetaData()
        
        self.sampleTable = {
            "name": 'MetricStats%s'%(self.metric)}
        self.sampleTable["table"] = Table(
            self.sampleTable["name"], 
            self.metadata,
            sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
            sqlalchemy.Column('SampleTime', sqlalchemy.DateTime(), primary_key=True),
            sqlalchemy.Column('Device',     sqlalchemy.String(length=40), primary_key=True),
            sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40), primary_key=True),
            sqlalchemy.Column('CumlativeMM',sqlalchemy.Float()),
            sqlalchemy.Column('SinceSec',   sqlalchemy.Float())
            )
        
        self.hourTable = {
            "name": 'MetricStatsHour%s'%(self.metric)}
        self.hourTable["table"] = Table(
            self.hourTable["name"], 
            self.metadata,
            sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
            sqlalchemy.Column('SampleTime', sqlalchemy.DateTime()),
            sqlalchemy.Column('Year',       sqlalchemy.Integer()),
            sqlalchemy.Column('Month',      sqlalchemy.Integer()),
            sqlalchemy.Column('Day',        sqlalchemy.Integer()),
            sqlalchemy.Column('Hour',       sqlalchemy.Integer()),
            sqlalchemy.Column('Device',     sqlalchemy.String(length=40)),
            sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40)),
            sqlalchemy.Column('CumlativeMM',sqlalchemy.Float()),
            sqlalchemy.Column('SinceSec',   sqlalchemy.Float()),
            sqlalchemy.Column('MaxSec',     sqlalchemy.Float()),
            sqlalchemy.Column('MinSec',     sqlalchemy.Float())
            )
        
        self.dayTable = {
            "name": 'MetricStatsDay%s'%(self.metric)}
        self.dayTable["table"] = Table(
            self.dayTable["name"], 
            self.metadata,
            sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
            sqlalchemy.Column('SampleTime', sqlalchemy.DateTime()),
            sqlalchemy.Column('Year',       sqlalchemy.Integer()),
            sqlalchemy.Column('Month',      sqlalchemy.Integer()),
            sqlalchemy.Column('Day',        sqlalchemy.Integer()),
            sqlalchemy.Column('Hour',       sqlalchemy.Integer()),
            sqlalchemy.Column('Device',     sqlalchemy.String(length=40)),
            sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40)),
            sqlalchemy.Column('CumlativeMM',sqlalchemy.Float()),
            sqlalchemy.Column('SinceSec',   sqlalchemy.Float()),
            sqlalchemy.Column('MaxSec',     sqlalchemy.Float()),
            sqlalchemy.Column('MinSec',     sqlalchemy.Float())
            )
        self.metadata.create_all(self.dbEng)
        
    def processDevice(self, device, sensor, getSample=_default_get_rain_cumlative, ts=None):
        if ts == None:
            ts = self.mostRecentTS(device, sensor)
            if ts == None:
                ts = pd.Timestamp(2001,1,1,0,0,0, tz="UTC")
        conn = self.dbEng.connect()
        sqlStm = (
            "SELECT * FROM DeviceSubmitRaw " +
            "WHERE DeviceId=\""+ device + "\" " +
            "AND  `Timestamp` > ('%s')" + 
            "ORDER BY `Timestamp` LIMIT 120;"
            )%(ts.strftime('%Y-%m-%d %X'))
        
        df = pd.read_sql(sqlStm, conn)
        metricDF = pd.DataFrame()
        aggReq = []
        for index, row in df.iterrows():
            j = json.loads(row["Payload"])
            header = j.get("header", {})
            t = header.get("timestamp", {})
            
            try:
                sampleTS = pd.Timestamp(
                  t.get("year", 2024),
                  t.get("month", 1),
                  t.get("day", 1),
                  t.get("hour", 0),
                  t.get("min", 0),
                  t.get("sec", 0),
                  tz="UTC"
                  )
                
                sample = getSample(j)
                if sample != None:
                    aggReq.append(sampleTS)
                    metricRow = {
                        "LoadTime":   row["Timestamp"],
                        "SampleTime": sampleTS,
                        "Device":     device,
                        "Sensor":     sensor,
                        "CumlativeMM":  sample.get("cumlative_mm", None),
                        "SinceSec":     sample.get("since_sec", None)
                        }
                    newRow = pd.DataFrame([metricRow])
                    metricDF = pd.concat([metricDF, newRow])
            except:
                print("Skipping invalid record %s"%header)
         
        metricDF.to_sql(self.sampleTable["name"], con=self.dbEng, if_exists='append', index=False)
        conn.close()
        
        count = len(aggReq)
        if (count > 0):
            self.hourAggregate(device, sensor, aggReq)
            self.dayAggregate(device, sensor, aggReq)
        return count
    
    def aggregate(self, device, sensor, todoDf, table):    
        conn = self.dbEng.connect()
        nRows=[]
        for index, row in todoDf.iterrows():
            start = row["start"]
            end   = row["end"] 
            stmt = select(
              func.max(self.sampleTable["table"].c.CumlativeMM),
              func.avg(self.sampleTable["table"].c.SinceSec),
              func.max(self.sampleTable["table"].c.SinceSec),
              func.min(self.sampleTable["table"].c.SinceSec)
            ).where(
                self.sampleTable["table"].c.Device == device
            ).where(
                self.sampleTable["table"].c.Sensor == sensor
            ).where(
                self.sampleTable["table"].c.SampleTime >= start.strftime('%Y-%m-%d %X')
            ).where(
                self.sampleTable["table"].c.SampleTime < end.strftime('%Y-%m-%d %X')
            )
            
            res = conn.execute(stmt)
            rows = res.all()
            (c, a, h, l) = rows[0]

            nRow = {
                "LoadTime":   pd.Timestamp.utcnow(),
                "SampleTime": start,
                "Year":       start.year,
                "Month":      start.month,
                "Day":        start.day,
                "Hour":       start.hour,
                "Device":     device,
                "Sensor":     sensor,
                "CumlativeMM":  c,
                "SinceSec":     a,
                "MaxSec":       h,
                "MinSec":       l
                }
            nRows.append(nRow)
            
            stmt = delete(
                table["table"]
                ).where(
                     table["table"].c.Device == device
                ).where(
                     table["table"].c.Sensor == sensor
                ).where(
                    table["table"].c.SampleTime == start.strftime('%Y-%m-%d %X')
                )
            res = conn.execute(stmt)
            conn.commit() 
            
        if len(nRows) > 0:
            df = pd.DataFrame(nRows)
            df.to_sql(table["name"], con=self.dbEng, if_exists='append', index=False)  
            
        #print(todoDf)
        
        return None
    
    def current(self, device, sensor, ts=None):
        if ts == None:
            ts = pd.Timestamp.utcnow()
        stmt = select(
            self.sampleTable["table"],
            self.sampleTable["table"].c.Sensor,
            self.sampleTable["table"].c.CumlativeMM,
            self.sampleTable["table"].c.SinceSec
          ).where(
              self.sampleTable["table"].c.Device == device
          ).where(
              self.sampleTable["table"].c.Sensor == sensor
          ). where(
              self.sampleTable["table"].c.SampleTime < ts.strftime('%Y-%m-%d %X')
          ).order_by(
              desc(self.sampleTable["table"].c.LoadTime)
          ).limit(1)
        #print(stmt)
        conn = self.dbEng.connect()
        df = pd.read_sql(stmt, conn)
        return df
        
    def hourly(self, device, sensor, startTS, endTS):
        
        stmt = select(
            self.hourTable["table"].c.SampleTime,
            self.hourTable["table"].c.Sensor,
            self.hourTable["table"].c.CumlativeMM,
            self.hourTable["table"].c.SinceSec,
            self.hourTable["table"].c.MaxSec,
            self.hourTable["table"].c.MinSec,
            (func.GREATEST(
                self.hourTable["table"].c.CumlativeMM -
                func.lag(
                    self.hourTable["table"].c.CumlativeMM
                ).over(
                    order_by=self.hourTable["table"].c.SampleTime
                ), 0
                )
            ).label('mmPerHour')
          ).where(
              self.hourTable["table"].c.Device == device
          ).where(
              self.hourTable["table"].c.Sensor == sensor
          ).where(
              self.hourTable["table"].c.SampleTime > startTS
          ).where(
              self.hourTable["table"].c.SampleTime < endTS
          ).order_by(
              self.hourTable["table"].c.SampleTime
          )
        conn = self.dbEng.connect()
        df = pd.read_sql(stmt, conn)
        return df