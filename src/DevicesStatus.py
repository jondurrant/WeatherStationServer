import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData, desc, func, inspect
import sqlalchemy
import json

class DevicesStatus:
    def __init__(self, dbEng):
        self.dbEng = dbEng

    def getDataFrame(self):
        conn = self.dbEng.connect()
        ts = pd.Timestamp.utcnow()
        sqlStm = (
            "SELECT Device, " +
            "TIMESTAMPDIFF(SECOND, `LastSeen`, ('%s')) as Seconds, "+
            "Status, Message, LastSeen, Type, Version "+
            "FROM DeviceStatus "+
            "WHERE `type` = \"WeatherStation\" " + 
            "AND `version` = 1 " +
            "ORDER BY `LastSeen`;")%ts.strftime('%Y-%m-%d %X')
        df = pd.read_sql(sqlStm, conn)
        return df
    
    def getWeatherStations(self):
        df = self.getDataFrame()
        return df.loc[:,'Device'].values