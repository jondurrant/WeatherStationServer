#!/usr/bin/env python # 
"""
 TODO
Usage: TODO
"""

from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
import sqlalchemy
import os
from MetricETemp import MetricETemp
from MetricSample import MetricSample



def picoGetSample(dic):
    return dic.get("pico", {}).get("celcius", None)

def ahtGetTemp(dic):
    return dic.get("aht10", {}).get("celcius", None)

def senGetTemp(dic):
    return dic.get("sen0500", {}).get("celcius", None)

if __name__ == "__main__":
   
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    #mTemp = MetricETemp(engine)
    #count = 10
    #while count > 0:
    #    print(mTemp.mostRecentTS("Test1"))
    #    count = mTemp.processDevice("Test1")
    #    print("Processed %d"%count)
    
    #mTemp.purge()
    
    
    picoTemp = MetricSample("ITemp", engine)
    print(picoTemp.mostRecentTS("Test1", "pico"))
    count = picoTemp.processDevice("Test1", "pico", picoGetSample)
    print("Processed %d"%count)
    
    ahtTemp = MetricSample("ETemp", engine)
    print(ahtTemp.mostRecentTS("Test1", "aht10"))
    count = ahtTemp.processDevice("Test1", "aht10", ahtGetTemp)
    print("Processed %d"%count)
    
    senTemp = MetricSample("ETemp", engine)
    print(senTemp.mostRecentTS("Test1", "sen0500"))
    count = senTemp.processDevice("Test1", "sen0500", senGetTemp)
    print("Processed %d"%count)