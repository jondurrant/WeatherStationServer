from dash import Dash, html,  dash_table, dcc
import dash_daq as daq
import pandas as pd
from sqlalchemy import create_engine, select, delete, Table, MetaData
from DevicesStatus import DevicesStatus
import os
from MetricSample import MetricSample
import plotly.express as px

app = Dash()





if __name__ == '__main__':
    #setup DB connection 
    dbHost=os.environ.get("DB_HOST", "localhost")
    dbPort=os.environ.get("DB_PORT", "3306")
    dbSchema=os.environ.get("DB_SCHEMA", "root")
    dbUser=os.environ.get("DB_USER", "root")
    dbPasswd=os.environ.get("DB_PASSWD", "root")
    connectString = "mysql+mysqlconnector://%s:%s@%s:%s/%s"%(dbUser, dbPasswd, dbHost, dbPort, dbSchema)
    engine = create_engine(connectString)
    
    devicesMgt = DevicesStatus(engine)
    df = devicesMgt.getDataFrame()
    devices = devicesMgt.getWeatherStations()
    
    
    deviceTemp = MetricSample("ETemp", engine)
    aht = deviceTemp.current("E6614104032F3F39", "aht10")
    
    #end = pd.Timestamp.utcnow() 
    end = pd.Timestamp(2024, 11, 10, 16, 00, 00)
    start = end - pd.Timedelta(days=1) 
    temp24Hours = deviceTemp.hourly("E6614104032F3F39", "aht10", start, end)
    print(temp24Hours)
    spark =  dcc.Graph(figure=px.line(temp24Hours, x='SampleTime', y='Sample'),
        style={
            "display": "inline-block",
            "width": "30%",
            "valign": "top"
        })
    
    them = daq.Thermometer(
        label='Current',
        labelPosition='top',
        value=aht['Sample'].values[0],
        scale={
            'start': 0, 
            'interval': 5,
            'labelInterval': 2, 
        },
        min = -10,
        max = 35,
        style={
            "display": "inline-block",
            "width": "10%"
        }
        )
    themMin = daq.Thermometer(
        label='Min',
        labelPosition='top',
        value=aht['Min'].values[0],
        scale={
            'start': 0, 
            'interval': 5,
            'labelInterval': 2, 
        },
        min = -10,
        max = 35,
        style={
            "display": "inline-block",
            "width": "10%"
        }
        )
    themMax = daq.Thermometer(
        label='Max',
        labelPosition='top',
        value=aht['Max'].values[0],
        scale={
            'start': 0, 
            'interval': 5,
            'labelInterval': 2, 
        },
        min = -10,
        max = 35,
        style={
            "display": "inline-block",
            "width": "10%"
        }
        )

    
    
    app.layout = [
        html.Div(children='Device Selection'),
        dcc.Dropdown(devices, devices[0], id='device'),
        them,
        themMin,
        themMax,
        spark,
        html.Div(children='My First App with Data'),
        dash_table.DataTable(
            data=df.to_dict('records'), 
            page_size=10, 
        style_data={
            'color': 'black',
            'backgroundColor': 'white'
        },
        style_data_conditional=[
            {
                'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(220, 220, 220)',
            }
        ],
        style_header={
            'backgroundColor': 'rgb(210, 100, 100)',
            'color': 'black',
            'fontWeight': 'bold'
        })
    ]
    
    app.run(debug=True)