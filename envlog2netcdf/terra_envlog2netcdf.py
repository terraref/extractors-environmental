#!/usr/bin/env python

import datetime
import os
import logging
import shutil
import subprocess
from netCDF4 import Dataset

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, download_metadata, upload_metadata
from pyclowder.files import upload_to_dataset
from terrautils.extractors import TerrarefExtractor, build_dataset_hierarchy, build_metadata, \
    is_latest_file, file_exists, contains_required_files
from terrautils.geostreams import get_sensor_by_name, create_datapoints, create_stream, \
    create_sensor, get_stream_by_name
from terrautils.metadata import get_extractor_metadata

import environmental_logger_json2netcdf as ela


def add_local_arguments(parser):
    # add any additional arguments to parser
    parser.add_argument('--batchsize', type=int, default=3000,
                        help="max number of datapoints to submit at a time")

def get_hour(filename):
    timestamp = filename.split('_')[1]
    hour = timestamp.split('-')[0]
    return int(hour)


class EnvironmentLoggerJSON2NetCDF(TerrarefExtractor):
    def __init__(self):
        super(EnvironmentLoggerJSON2NetCDF, self).__init__()

        # add any additional arguments to parser
        add_local_arguments(self.parser)

        # parse command line and load default logging configuration
        self.setup(sensor='envlog_netcdf')

        self.batchsize = self.args.batchsize

    def check_message(self, connector, host, secret_key, resource, parameters):
        if "rulechecked" in parameters and parameters["rulechecked"]:
            return CheckMessage.download

        if not is_latest_file(resource):
            self.log_skip(resource, "not latest file")
            return CheckMessage.ignore

        if len(resource['files'] >= 23):
            md = download_metadata(connector, host, secret_key, resource['id'])
            if get_extractor_metadata(md, self.extractor_info['name'], self.extractor_info['version']):
                timestamp = resource['name'].split(" - ")[1]
                out_fullday_netcdf = self.sensors.create_sensor_path(timestamp)
                if file_exists(out_fullday_netcdf):
                    self.log_skip(resource, "metadata v%s and outputs already exist" % self.extractor_info['version'])
                    return CheckMessage.ignore
            return CheckMessage.download
        else:
            self.log_skip(resource, "found less than 23 files")
            return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message(resource)

        # Build list of JSON files
        json_files = []
        for f in resource['files']:
            if f['filename'].endswith("_environmentlogger.json"):
                json_files.append(f['filepath'])
        json_files.sort()

        # Determine full output path
        timestamp = resource['name'].split(" - ")[1]
        out_fullday_netcdf = self.sensors.create_sensor_path(timestamp)

        temp_out_full = "temp_full.nc"
        temp_out = "temp.nc"
        for json_file in json_files:
            self.log_info(resource, "converting %s to netCDF & appending" % json_file)
            ela.mainProgramTrigger(json_file, temp_out)
            cmd = "ncrcat --record_append %s %s" % (temp_out, temp_out_full)
            subprocess.call([cmd], shell=True)
            os.remove(temp_out)

        shutil.move(temp_out_full, out_fullday_netcdf)
        self.created += 1
        self.bytes += os.path.getsize(out_fullday_netcdf)

        # Write out geostreams.csv
        self.log_info(resource, "writing geostreams CSV")
        prepareDatapoint(connector, host, secret_key, resource, out_fullday_netcdf, self.batchsize)

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
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        self.end_message(resource)


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
