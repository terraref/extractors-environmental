#!/usr/bin/env python

import datetime
import os
import shutil
import subprocess
import json
from netCDF4 import Dataset

from pyclowder.utils import CheckMessage
from pyclowder.datasets import get_info, get_file_list, download_metadata, upload_metadata
from pyclowder.files import upload_to_dataset, submit_extraction
from terrautils.extractors import TerrarefExtractor, build_dataset_hierarchy_crawl, build_metadata, \
    is_latest_file, file_exists, contains_required_files
from terrautils.metadata import get_extractor_metadata

import environmental_logger_json2netcdf as ela


def add_local_arguments(parser):
    # add any additional arguments to parser
    parser.add_argument('--batchsize', type=int, default=3000,
                        help="max number of datapoints to submit at a time")

def _produce_attr_dict(netCDF_variable_obj):
    '''
    Produce a list of dictionary with attributes and value (Each dictionary is one datapoint)
    '''
    attributes = [attr for attr in dir(netCDF_variable_obj) if isinstance(attr, unicode)]
    result     = {name:getattr(netCDF_variable_obj, name) for name in attributes}

    return [dict(result.items()+ {"value":str(data)}.items()) for data in netCDF_variable_obj[...]]

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

        if len(resource['files']) >= 23:
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
                if f['filepath'].startswith("/home/clowder"):
                    json_files.append(f['filepath'].replace("/home/clowder", "/home/extractor"))
                else:
                    json_files.append(f['filepath'])
        json_files.sort()

        # Determine full output path
        timestamp = resource['name'].split(" - ")[1]
        out_fullday_netcdf = self.sensors.create_sensor_path(timestamp)
        temp_out_full = os.path.join(os.path.dirname(out_fullday_netcdf), "temp_full.nc")
        temp_out_single = temp_out_full.replace("_full.nc", "_single.nc")

        for json_file in json_files:
            self.log_info(resource, "converting %s to netCDF & appending" % os.path.basename(json_file))
            ela.mainProgramTrigger(json_file, temp_out_single)
            cmd = "ncrcat --record_append %s %s" % (temp_out_single, temp_out_full)
            subprocess.call([cmd], shell=True)
            os.remove(temp_out_single)

        shutil.move(temp_out_full, out_fullday_netcdf)
        self.created += 1
        self.bytes += os.path.getsize(out_fullday_netcdf)

        # Write out geostreams.csv
        self.log_info(resource, "writing geostreams CSV")
        geo_csv = out_fullday_netcdf.replace(".nc", "_geo.csv")
        geo_file = open(geo_csv, 'w')
        geo_file.write(','.join(['site', 'trait', 'lat', 'lon', 'dp_time', 'source', 'value', 'timestamp']) + '\n')
        with Dataset(out_fullday_netcdf, "r") as ncdf:
            streams = set([sensor_info.name for sensor_info in ncdf.variables.values() if sensor_info.name.startswith('sensor')])
            for stream in streams:
                if stream != "sensor_spectrum":
                    try:
                        memberlist = ncdf.get_variables_by_attributes(sensor=stream)
                        for members in memberlist:
                            data_points = _produce_attr_dict(members)
                            for index in range(len(data_points)):
                                dp_obj = data_points[index]
                                if dp_obj["sensor"] == stream:
                                    time_format = "%Y-%m-%dT%H:%M:%S-07:00"
                                    time_point = (datetime.datetime(year=1970, month=1, day=1) + \
                                                  datetime.timedelta(days=ncdf.variables["time"][index])).strftime(time_format)

                                    geo_file.write(','.join(["Full Field - Environmental Logger",
                                                             "(EL) %s" % stream,
                                                             str(33.075576),
                                                             str(-111.974304),
                                                             time_point,
                                                             host + ("" if host.endswith("/") else "/") + "datasets/" + resource['id'],
                                                             '"%s"' % json.dumps(dp_obj).replace('"', '""'),
                                                             timestamp]) + '\n')

                    except:
                        self.log_error(resource, "NetCDF attribute not found: %s" % stream)

        # Fetch dataset ID by dataset name if not provided
        target_dsid = build_dataset_hierarchy_crawl(host, secret_key, self.clowder_user, self.clowder_pass, self.clowderspace,
                                                    None, None, self.sensors.get_display_name(),
                                                    timestamp[:4], timestamp[5:7], timestamp[8:10],
                                                    leaf_ds_name=self.sensors.get_display_name() + ' - ' + timestamp)
        ds_files = get_file_list(connector, host, secret_key, target_dsid)
        found_full = False
        found_csv  = False
        for f in ds_files:
            if f['filename'] == os.path.basename(out_fullday_netcdf):
                found_full = True
            if f['filename'] == os.path.basename(geo_csv):
                found_csv = True
        if not found_full:
            upload_to_dataset(connector, host, secret_key, target_dsid, out_fullday_netcdf)
        if not found_csv:
            geoid = upload_to_dataset(connector, host, secret_key, target_dsid, geo_csv)
            self.log_info(resource, "triggering geostreams extractor on %s" % geoid)
            submit_extraction(connector, host, secret_key, geoid, "terra.geostreams")

        # Tell Clowder this is completed so subsequent file updates don't daisy-chain
        ext_meta = build_metadata(host, self.extractor_info, resource['id'], {
            "output_dataset": target_dsid
        }, 'dataset')
        upload_metadata(connector, host, secret_key, resource['id'], ext_meta)

        self.end_message(resource)

if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
