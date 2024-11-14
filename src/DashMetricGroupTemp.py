from DashMetricGroup import  DashMetricGroup
import dash_daq as daq

class DashMetricGroupTemp(DashMetricGroup):
    
    def setUnits(self, units):
        self.units = "C"
    
    def getGuage(self, title, value = 0):
        return daq.Thermometer(
            label=title,
            labelPosition='top',
            value=value,
            width=10,
            scale={
                'start': 0, 
                'interval': 5,
                'labelInterval': 2,
                'style': {
                    'fontSize':40
                    }
            },
            min = -10,
            max = 35,
            style= self.style,
            color='red',
            units="C",
            id=self.id + "guage"
            )