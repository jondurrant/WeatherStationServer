from flask import Flask,flash, redirect, request, send_from_directory
from flask import url_for, render_template
import json
import os
import sys
from dotmap import DotMap
from DevicesStatus import DevicesStatus
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData

# set the project root directory as the static folder, you can set others.
app = Flask(__name__, static_url_path='/static', 
            static_folder='static')

@app.route('/')
def route():
    return redirect("/static/index.html")
    #return redirect("/static/html/Network.html")


@app.route('/devicesStatus', methods=['GET', 'POST'])
def getDevicesStatus():
    devicesMgt = DevicesStatus(engine)
    df = devicesMgt.getDataFrame()
    t = frameToJson(df)
    print(json.dumps(t.toDict()))
    return json.dumps(t.toDict())

typeConvert = {
        'string':   lambda s: str(s),
        'int':      lambda i: int(i),
        'int64':    lambda i: int(i),
        'int32':    lambda i: int(i),
        'float':    lambda f: float(f),
        "float64":  lambda f: float(f),
        'boolean':  lambda b: bool(b),
        "datetime64[ns]": lambda d: d.strftime("%d-%b-%Y, %H:%M:%S")
    }

typeDataTable = {
        'int':      "number",
        'int64':    "number",
        'int32':    "number",
        'float':    "number",
        "float64":  "number",
        "object":   "string",
        "bool":     "boolean",
        "datetime64[ns]": "string"
    }

def frameToColumnJson(frame):
    res = DotMap({
        "cols":[{
            "id": "LabeL",
            "label": "Label",
            "type": "string"    
        }],
        "rows":[]
    })
    
    #Build columns
    cl = frame.columns
    
    for index, row in frame.iterrows():
        res.cols.append({
            "id": str(index),
            "label": str(index),
            "type": "string"  
        })
    #Build the rows  
    for c in cl:  
        jrow = [{"v": c}]
        for index, row in frame.iterrows():
            #print(frame[c].dtypes)
            v = typeConvert.get(str(frame[c].dtypes), lambda a: str(a))(row[c])
            jrow.append({"v": v})
        res.rows.append({"c": jrow})
    return res


def frameToJson(frame):
    res = DotMap({
        "cols":[{
            "id": "Index",
            "label": "Index",
            "type": "string"    
        }],
        "rows":[]
    })
    
    #print(res)
    
    #Build columns
    l = len(frame.dtypes)
    dt = frame.dtypes
    cl = frame.columns
    
    for i in range(l):
        t = str(dt.iloc[i])
        ty = typeDataTable.get(t, t)
        res.cols.append({
            "id": cl[i],
            "label": cl[i],
            "type": ty    
        })
        
    #print(res)
    
    
    #Build the rows    
    for index, row in frame.iterrows():
        jrow = [{"v":str(index)}]
        for c in cl:
            #print(frame[c].dtypes)
            v = typeConvert.get(str(frame[c].dtypes), lambda a: str(a))(row[c])
            jrow.append({"v": v})
        res.rows.append({"c": jrow})
        
    #print(res)
    
    
    return res
    



if __name__ == "__main__":
    app.secret_key = 'LCARS'
    app.config['SESSION_TYPE'] = 'filesystem'
    
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    global engine 
    engine = create_engine(connectString)
    
    
    app.run(host="0.0.0.0", port="5080")
