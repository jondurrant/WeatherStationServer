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




if __name__ == "__main__":
   
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    mTemp = MetricETemp(engine)

    mTemp.processDevice("FAKE1")