#!/usr/bin/env python

import datetime
import os
import logging
from netCDF4 import Dataset

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list
from pyclowder.files import upload_to_dataset
from terrautils.extractors import TerrarefExtractor, build_dataset_hierarchy
from terrautils.geostreams import get_sensor_by_name, create_datapoint, create_stream, \
    create_sensor, get_stream_by_name

import environmental_logger_json2netcdf as ela


class EnvironmentLoggerJSON2NetCDF(TerrarefExtractor):
    def __init__(self):
        super(EnvironmentLoggerJSON2NetCDF, self).__init__()

        # parse command line and load default logging configuration
        self.setup(sensor='envlog_netcdf')

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Only trigger extraction if the newly added file is a relevant JSON file
        if not resource['name'].endswith("_environmentlogger.json"):
            return CheckMessage.ignore

        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        out_netcdf = self.create_sensor_path(timestamp, hms=resource['name'][11:19])

        if os.path.isfile(out_netcdf) and not self.overwrite:
            return CheckMessage.ignore

        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        # path to input JSON file
        in_envlog = resource['local_paths'][0]
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        out_netcdf = self.create_sensor_path(timestamp, hms=resource['name'][11:19])

        # Create netCDF if it doesn't exist
        if not os.path.isfile(out_netcdf) or self.overwrite:
            logging.info("converting JSON to: %s" % out_netcdf)
            ela.mainProgramTrigger(in_envlog, out_netcdf)

            self.created_count += 1
            self.bytes += os.path.getsize(out_netcdf)

            # Fetch dataset ID by dataset name if not provided
            logging.info("uploading netCDF file to Clowder")
            target_dsid = build_dataset_hierarchy(connector, host, secret_key, self.clowderspace,
                                      self.sensors.get_display_name(), timestamp[:4], timestamp[5:7],
                                      leaf_ds_name=resource['dataset_info']['name'])
            upload_to_dataset(connector, host, secret_key, target_dsid, out_netcdf)

            # Push to geostreams
            prepareDatapoint(connector, host, secret_key, resource, out_netcdf)

        else:
            logging.info("%s already exists; skipping" % out_netcdf)

        self.end_message()

def _produce_attr_dict(netCDF_variable_obj):
    '''
    Produce a list of dictionary with attributes and value (Each dictionary is one datapoint)
    '''
    attributes = [attr for attr in dir(netCDF_variable_obj) if isinstance(attr, unicode)]
    result     = {name:getattr(netCDF_variable_obj, name) for name in attributes}

    return [dict(result.items()+ {"value":str(data)}.items()) for data in netCDF_variable_obj[...]]

def prepareDatapoint(connector, host, secret_key, resource, ncdf):
    # TODO: Get this from Clowder
    coords = [-111.974304, 33.075576, 0]
    geom = {
        "type": "Point",
        "coordinates": coords
    }

    with Dataset(ncdf, "r") as netCDF_handle:
        sensor_name = "Full Field - Environmental Logger"
        sensor_data = get_sensor_by_name(connector, host, secret_key, sensor_name)
        if not sensor_data:
            sensor_id = create_sensor(connector, host, secret_key, sensor_name, geom)
        else:
            sensor_id = sensor_data['id']

        stream_list = set([sensor_info.name for sensor_info in netCDF_handle.variables.values() if sensor_info.name.startswith('sensor')])
        for stream in stream_list:
            stream_name = "EnvLog %s - Full Field" % stream
            stream_data = get_stream_by_name(connector, host, secret_key, stream_name)
            if not stream_data:
                stream_id = create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
            else:
                stream_id = stream_data['id']

            try:
                memberlist = netCDF_handle.get_variables_by_attributes(sensor=lambda x: x is not None)
                for members in memberlist:
                    data_points = _produce_attr_dict(members)

                    for index in range(len(data_points)):
                        time_format = "%Y-%m-%dT%H:%M:%S-07:00"
                        time_point = (datetime.datetime(year=1970, month=1, day=1) + \
                                      datetime.timedelta(days=netCDF_handle.variables["time"][index])).strftime(time_format)

                        create_datapoint(connector, host, secret_key, stream_id, geom,
                                                              time_point, time_point, data_points[index])
            except:
                logging.error("NetCDF attribute not found: %s" % stream)

if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
