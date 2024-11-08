import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData, desc, func, inspect
import sqlalchemy
import json

class MetricETemp:
  def __init__(self, dbEng):
    self.dbEng = dbEng
    
    self.metadata=MetaData()
    self.MetricStatsETemp = Table(
        'MetricStatsETemp', 
        self.metadata,
        sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
        sqlalchemy.Column('SampleTime', sqlalchemy.DateTime(), primary_key=True),
        sqlalchemy.Column('Device',     sqlalchemy.String(length=40), primary_key=True),
        sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40), primary_key=True),
        sqlalchemy.Column('Sample',     sqlalchemy.Float()),
        sqlalchemy.Column('Min',        sqlalchemy.Float()),
        sqlalchemy.Column('Max',        sqlalchemy.Float()),
        )
    self.MetricStatsHourETemp = Table(
        'MetricStatsHourETemp', 
        self.metadata,
        sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
        sqlalchemy.Column('SampleTime', sqlalchemy.DateTime()),
        sqlalchemy.Column('Year',       sqlalchemy.Integer()),
        sqlalchemy.Column('Month',      sqlalchemy.Integer()),
        sqlalchemy.Column('Day',        sqlalchemy.Integer()),
        sqlalchemy.Column('Hour',       sqlalchemy.Integer()),
        sqlalchemy.Column('Device',     sqlalchemy.String(length=40)),
        sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40)),
        sqlalchemy.Column('Sample',     sqlalchemy.Float()),
        sqlalchemy.Column('Min',        sqlalchemy.Float()),
        sqlalchemy.Column('Max',        sqlalchemy.Float()),
        )
    self.MetricStatsDayETemp = Table(
        'MetricStatsDayETemp', 
        self.metadata,
        sqlalchemy.Column('LoadTime',   sqlalchemy.DateTime()),
        sqlalchemy.Column('SampleTime', sqlalchemy.DateTime()),
        sqlalchemy.Column('Year',       sqlalchemy.Integer()),
        sqlalchemy.Column('Month',      sqlalchemy.Integer()),
        sqlalchemy.Column('Day',        sqlalchemy.Integer()),
        sqlalchemy.Column('Hour',       sqlalchemy.Integer()),
        sqlalchemy.Column('Device',     sqlalchemy.String(length=40)),
        sqlalchemy.Column('Sensor',     sqlalchemy.String(length=40)),
        sqlalchemy.Column('Sample',     sqlalchemy.Float()),
        sqlalchemy.Column('Min',        sqlalchemy.Float()),
        sqlalchemy.Column('Max',        sqlalchemy.Float()),
        )
    self.metadata.create_all(dbEng)


  def processDevice(self, device, ts=None):
      if ts == None:
          ts = self.mostRecentTS(device)
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
          aht10 = j.get("aht10", None)
          sampleTS = pd.Timestamp(
            t.get("year", 2024),
            t.get("month", 1),
            t.get("day", 1),
            t.get("hour", 0),
            t.get("min", 0),
            t.get("sec", 0),
            tz="UTC"
            )
          aggReq.append(sampleTS)
          if aht10 != None:
              metricRow = {
                  "LoadTime":   row["Timestamp"],
                  "SampleTime": sampleTS,
                  "Device":     device,
                  "Sensor":     "AHT10",
                  "Sample":     aht10.get("celcius", None),
                  "Min":        aht10.get("min_celcius", None),
                  "Max":        aht10.get("max_celcius", None)
                  }
              newRow = pd.DataFrame([metricRow])
              metricDF = pd.concat([metricDF, newRow])
       
      metricDF.to_sql('MetricStatsETemp', con=self.dbEng, if_exists='append', index=False)
      conn.close()
      
      count = len(aggReq)
      if (count > 0):
          self.hourAggregate(device, aggReq)
          self.dayAggregate(device, aggReq)
      return count
      
      
  def hourAggregate(self, device, aggRequired):
      todoList = []
      for t in aggRequired:
          ts = pd.Timestamp(t.year, t.month, t.day, t.hour)
          todoList.append({
              "start":  ts,
              "end":    ts + pd.Timedelta(hours=1)
              })
      todoDf = pd.DataFrame(todoList).drop_duplicates()
      
      self.aggregate(device, todoDf, self.MetricStatsHourETemp, 'MetricStatsHourETemp')
      
      
  def dayAggregate(self, device, aggRequired):
      todoList = []
      for t in aggRequired:
          ts = pd.Timestamp(t.year, t.month, t.day)
          todoList.append({
              "start":  ts,
              "end":    ts + pd.Timedelta(days=1)
              })
      todoDf = pd.DataFrame(todoList).drop_duplicates()
      
      self.aggregate(device, todoDf, self.MetricStatsDayETemp, 'MetricStatsDayETemp')    
  
  def aggregate(self, device, todoDf, table, tableName):    
      conn = self.dbEng.connect()
      nRows=[]
      for index, row in todoDf.iterrows():
          start = row["start"]
          end   = row["end"] 
          stmt = select(
            func.avg(self.MetricStatsETemp.c.Sample),
            func.min(self.MetricStatsETemp.c.Min),
            func.max(self.MetricStatsETemp.c.Max)
          ).where(
              self.MetricStatsETemp.c.Device == device
          ).where(
              self.MetricStatsETemp.c.SampleTime >= start.strftime('%Y-%m-%d %X')
          ).where(
              self.MetricStatsETemp.c.SampleTime < end.strftime('%Y-%m-%d %X')
          )
          
          res = conn.execute(stmt)
          rows = res.all()
          (s, mi, ma) = rows[0]
          nRow = {
              "LoadTime":   pd.Timestamp.utcnow(),
              "SampleTime": start,
              "Year":       start.year,
              "Month":      start.month,
              "Day":        start.day,
              "Hour":       start.hour,
              "Device":     device,
              "Sensor":     "AHT10",
              "Sample":     s,
              "Min":        mi,
              "Max":        ma
              }
          nRows.append(nRow)
          
          stmt = delete(
              table
              ).where(
                   table.c.Device == device
              ).where(
                   table.c.Sensor == "AHT10"
              ).where(
                  table.c.SampleTime == start.strftime('%Y-%m-%d %X')
              )
          res = conn.execute(stmt)
          conn.commit() 
          
      if len(nRows) > 0:
          df = pd.DataFrame(nRows)
          df.to_sql(tableName, con=self.dbEng, if_exists='append', index=False)  
          
      #print(todoDf)
    
      return None
      
  def mostRecentTS(self, device):
        stmt = select(
            self.MetricStatsETemp,
            self.MetricStatsETemp.c.LoadTime
          ).where(
              self.MetricStatsETemp.c.Device == device
          ).order_by(
              desc(self.MetricStatsETemp.c.LoadTime)
          ).limit(1)
        conn = self.dbEng.connect()
        res = conn.execute(stmt)
        rows = res.all()
        count = len(rows)
        if (count == 0):
            return None
        ts = rows[0][0]
        conn.close()
        return ts
    
  def purge(self, baseDays = 28, hourDays = 365, dayDays = 365*5):
      conn = self.dbEng.connect()
      
      #Base
      ts = pd.Timestamp.utcnow() - pd.Timedelta(days=baseDays)
      stmt = delete(
           self.MetricStatsETemp
          ).where(
              self.MetricStatsETemp.c.SampleTime < ts.strftime('%Y-%m-%d %X')
          )
      conn.execute(stmt)
      
      #Hour
      ts = pd.Timestamp.utcnow() - pd.Timedelta(days=hourDays)
      stmt = delete(
           self.MetricStatsHourETemp
          ).where(
              self.MetricStatsHourETemp.c.SampleTime < ts.strftime('%Y-%m-%d %X')
          )
      conn.execute(stmt)
      
      #Days
      ts = pd.Timestamp.utcnow() - pd.Timedelta(days=dayDays)
      stmt = delete(
           self.MetricStatsDayETemp
          ).where(
              self.MetricStatsDayETemp.c.SampleTime < ts.strftime('%Y-%m-%d %X')
          )
      conn.execute(stmt)
      conn.commit()
          
        
      
