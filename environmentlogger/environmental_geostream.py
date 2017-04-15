import json
import functools
import numpy as np
from pyclowder.geostreams import *
from pyclowder.connectors import Connector
from netCDF4  import Dataset
from datetime import datetime, timedelta


_HOST_URL = "https://terraref.ncsa.illinois.edu/clowder/"

def _produce_attr_dict(netCDF_variable_obj):
    '''
    Produce a list of dictionary with attributes and value (Each dictionary is one datapoint)
    '''
    attributes = [attr for attr in dir(netCDF_variable_obj) if isinstance(attr, unicode)]
    result     = {name:getattr(netCDF_variable_obj, name) for name in attributes}

    return [dict(result.items()+ {"value":str(data)}.items()) for data in netCDF_variable_obj[...]]

def push_to_geostream(sensor_name):
    '''
    Will push the data from output environmental logger file into the Clowder geostream

    1st Level is the Environmental Logger itself
    2nd Level is the sensors (weather station, sensor co2, etc.)
    3rd Level is the data collected in each sensor at each time point
    '''
    def decorator(func):
        @functools.wraps(func)
        def _push_to_geostream(*args, **kwargs):
            '''
            Will do the real work: setup sensor, stream and datapoints
            '''
            file_path, API_key = func(*args, **kwargs)

            if API_key is None:
                print "This execution will not be pushed to the Clowder Geostream"
            else:
                if sensor_name == "Full Field - Environmental Logger":
                    coordinate_set = [-111.974304, 33.075576, 0] #FIXME Is it [lat, long, sea_lvl]? How to represent an area
                
                custom_arguments = {"geom": {"type":       "Area",
                                             "coordinates":coordinate_set},
                                    "type":  {"id"        :"MAC Field Scanner",
                                              "title"     :"MAC Field Scanner",
                                              "sensorType": 4},
                                    "region": "Maricopa"}

                time_format = "%Y-%m-%dT%H:%M:%s-06:00" #FIXME What is -06:00?

                with Dataset(file_path, "r") as netCDF_handle:
                     puppet_connector = Connector(sensor_name)

                     ### 1st Level (Plot) ###
                     sensor_id = create_sensor(puppet_connector, 
                                               _HOST_URL, API_key, 
                                               sensor_name, 
                                               **custom_arguments)

                     stream_list = set([getattr(data, u'sensor') for data in netCDF_handle.variables.values() if u'sensor' in dir(data)])

                     ### 2nd Level (Stream) "Sensors" in Environmental Logger ###
                     for stream in stream_list:
                         stream_id = create_stream(puppet_connector,
                                                   _HOST_URL, 
                                                   API_key, 
                                                   str(stream), 
                                                   sensor_id, 
                                                   custom_arguments["geom"])

                         for members in netCDF_handle.get_variables_by_attributes(sensor=stream):
                            data_points = _produce_attr_dict(members)

                            for index in range(len(data_points)):
                                time_point = (datetime(year=1970, month=1, day=1) +\
                                              timedelta(days=netCDF_handle.variables["time"][index])).strftime(time_format)

                                ### 3rd Level (data_point) data collected in each time point ###
                                ### FIXME Internal Server Error 500 Anything wrong?
                                data_point_id = create_datapoint(puppet_connector, 
                                                                 _HOST_URL, 
                                                                 API_key, 
                                                                 stream_id,
                                                                 custom_arguments["geom"],
                                                                 time_point, ### <-- starttime = endtime because collected in one moment
                                                                 time_point, ### <-- starttime = endtime because collected in one moment
                                                                 data_points[index]) ### <-- The attribute and values in each time point (data_point)
        return _push_to_geostream
    return decorator