#!/usr/bin/env python

import datetime
import os
import logging
import shutil
import subprocess
import time
from netCDF4 import Dataset

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list
from pyclowder.files import upload_to_dataset, upload_metadata, download_metadata
from terrautils.extractors import TerrarefExtractor, build_dataset_hierarchy, build_metadata
from terrautils.geostreams import get_sensor_by_name, create_datapoints, create_stream, \
    create_sensor, get_stream_by_name

import environmental_logger_json2netcdf as ela


def add_local_arguments(parser):
    # add any additional arguments to parser
    parser.add_argument('--batchsize', type=int, default=3000,
                        help="max number of datapoints to submit at a time")

class EnvironmentLoggerJSON2NetCDF(TerrarefExtractor):
    def __init__(self):
        super(EnvironmentLoggerJSON2NetCDF, self).__init__()

        # add any additional arguments to parser
        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='envlog_netcdf')

        self.batchsize = self.args.batchsize

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Only trigger extraction if the newly added file is a relevant JSON file
        if not resource['name'].endswith("_environmentlogger.json"):
            return CheckMessage.ignore

        file_md = download_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
        if len(file_md) > 0:
            # This file was already processed
            return CheckMessage.ignore

        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        # path to input JSON file
        in_envlog = resource['local_paths'][0]
        ds_info = get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = ds_info['name'].split(" - ")[1]
        out_temp = "temp_output.nc"
        out_fullday_netcdf = self.sensors.create_sensor_path(timestamp)
        lockfile = out_fullday_netcdf.replace(".nc", ".lock")

        logging.info("converting JSON to: %s" % out_temp)
        ela.mainProgramTrigger(in_envlog, out_temp)

        self.created += 1
        self.bytes += os.path.getsize(out_temp)

        # Merge this chunk into full day
        if not os.path.exists(out_fullday_netcdf):
            shutil.move(out_temp, out_fullday_netcdf)

            # Push to geostreams
            prepareDatapoint(connector, host, secret_key, resource, out_fullday_netcdf)
        else:
            # Create lockfile to make sure we don't step on each others' toes
            total_wait = 0
            max_wait_mins = 10
            while os.path.exists(lockfile):
                time.sleep(1)
                total_wait += 1
                if total_wait > max_wait_mins*60:
                    logging.error("wait time for %s exceeded %s minutes, unlocking" % (lockfile, max_wait_mins))
                    os.remove(lockfile)

            open(lockfile, 'w').close()
            try:
                cmd = "ncrcat --record_append %s %s" % (out_temp, out_fullday_netcdf)
                subprocess.call([cmd], shell=True)
            finally:
                os.remove(lockfile)

            # Push to geostreams
            prepareDatapoint(connector, host, secret_key, resource, out_temp, self.batchsize)
            os.remove(out_temp)

        # Fetch dataset ID by dataset name if not provided
        target_dsid = build_dataset_hierarchy(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                  self.sensors.get_display_name(), timestamp[:4], timestamp[5:7],
                                  leaf_ds_name=self.sensors.get_display_name()+' - '+timestamp)
        ds_files = get_file_list(connector, host, secret_key, target_dsid)
        found_full = False
        for f in ds_files:
            if f['filename'] == os.path.basename(out_fullday_netcdf):
                found_full = True
        if not found_full:
            upload_to_dataset(connector, host, secret_key, target_dsid, out_fullday_netcdf)

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
            "output_dataset": target_dsid
        }, 'file')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        self.end_message()

def _produce_attr_dict(netCDF_variable_obj):
    '''
    Produce a list of dictionary with attributes and value (Each dictionary is one datapoint)
    '''
    attributes = [attr for attr in dir(netCDF_variable_obj) if isinstance(attr, unicode)]
    result     = {name:getattr(netCDF_variable_obj, name) for name in attributes}

    return [dict(result.items()+ {"value":str(data)}.items()) for data in netCDF_variable_obj[...]]

def prepareDatapoint(connector, host, secret_key, resource, ncdf, batchsize):
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
            sensor_id = create_sensor(connector, host, secret_key, sensor_name, geom,
                                      {"id": "MAC Field Scanner", "title": "MAC Field Scanner", "sensorType": 4},
                                      "Maricopa")
        else:
            sensor_id = sensor_data['id']

        stream_list = set([sensor_info.name for sensor_info in netCDF_handle.variables.values() if sensor_info.name.startswith('sensor')])
        for stream in stream_list:
            if stream != "sensor_spectrum":
                stream_name = "(EL) %s" % stream
                stream_data = get_stream_by_name(connector, host, secret_key, stream_name)
                if not stream_data:
                    stream_id = create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
                else:
                    stream_id = stream_data['id']

                try:
                    memberlist = netCDF_handle.get_variables_by_attributes(sensor=stream)
                    for members in memberlist:
                        data_points = _produce_attr_dict(members)
                        data_point_list = []
                        for index in range(len(data_points)):
                            dp_obj = data_points[index]
                            if dp_obj["sensor"] == stream:
                                time_format = "%Y-%m-%dT%H:%M:%S-07:00"
                                time_point = (datetime.datetime(year=1970, month=1, day=1) + \
                                              datetime.timedelta(days=netCDF_handle.variables["time"][index])).strftime(time_format)

                                data_point_list.append({
                                    "start_time": time_point,
                                    "end_time": time_point,
                                    "type": "Point",
                                    "geometry": geom,
                                    "properties": dp_obj
                                })

                                if len(data_point_list) > batchsize:
                                    create_datapoints(connector, host, secret_key, stream_id, data_point_list)
                                    data_point_list = []

                        if len(data_point_list) > 0:
                            create_datapoints(connector, host, secret_key, stream_id, data_point_list)
                except:
                    logging.error("NetCDF attribute not found: %s" % stream)

if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
