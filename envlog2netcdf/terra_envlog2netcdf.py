#!/usr/bin/env python

import datetime
import os
import logging
import requests

from netCDF4  import Dataset

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import pyclowder.geostreams
import terrautils.extractors

import environmental_logger_json2netcdf as ela


class EnvironmentLoggerJSON2NetCDF(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
        influx_port = os.getenv("INFLUXDB_PORT", 8086)
        influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
        influx_user = os.getenv("INFLUXDB_USER", "terra")
        influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

        # add any additional arguments to parser
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/EnvironmentLogger",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")
        self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                                 default=influx_host, help="InfluxDB URL for logging")
        self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                                 default=influx_port, help="InfluxDB port")
        self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                                 default=influx_user, help="InfluxDB username")
        self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                                 default=influx_pass, help="InfluxDB password")
        self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                                 default=influx_db, help="InfluxDB database")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite
        self.influx_params = {
            "host": self.args.influx_host,
            "port": self.args.influx_port,
            "db": self.args.influx_db,
            "user": self.args.influx_user,
            "pass": self.args.influx_pass
        }

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Only trigger extraction if the newly added file is a relevant JSON file
        if not resource['name'].endswith("_environmentlogger.json"):
            return CheckMessage.ignore

        return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created_count = 0
        bytes = 0

        # path to input JSON file
        in_envlog = resource['local_paths'][0]

        ds_info = pyclowder.datasets.get_info(connector, host, secret_key, resource['parent']['id'])
        timestamp = resource['name'].split("_")[0]
        out_dir = os.path.join(self.output_dir, timestamp)
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        out_filename = terrautils.extractors.get_output_filename(ds_info['name'], 'nc', hms=resource['name'][11:19])
        out_netcdf = os.path.join(out_dir, out_filename)

        # Create netCDF if it doesn't exist
        if not os.path.isfile(out_netcdf) or self.force_overwrite:
            logging.info("converting JSON to: %s" % out_netcdf)
            ela.mainProgramTrigger(in_envlog, out_netcdf)

            created_count += 1
            bytes += os.path.getsize(out_netcdf)

            # Fetch dataset ID by dataset name if not provided
            if resource['parent']['id'] == '':
                ds_name = 'EnvironmentLogger - ' + resource['name'].split('_')[0]
                url = '%s/api/datasets?key=%s&title=%s' % (host, secret_key, ds_name)
                r = requests.get(url, headers={'Content-Type': 'application/json'})
                if r.status_code == 200:
                    resource['parent']['id'] = r.json()[0]['id']
            else:
                ds_files = pyclowder.datasets.get_file_list(connector, host, secret_key, resource['parent']['id'])
                found_output_in_dataset = False
                for f in ds_files:
                    if f['filename'] == out_filename:
                        found_output_in_dataset = True
                if not found_output_in_dataset:
                    logging.info("uploading netCDF file to Clowder")
                    pyclowder.files.upload_to_dataset(connector, host, secret_key,
                                                  resource['parent']['id'], out_netcdf)
                else:
                    logging.info("%s already in Clowder; not re-uploading" % out_netcdf)

            # Push to geostreams
            prepareDatapoint(connector, host, secret_key, resource, out_netcdf)

        else:
            logging.info("%s already exists; skipping" % out_netcdf)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        terrautils.extractors.log_to_influxdb(self.extractor_info['name'], self.influx_params,
                                              starttime, endtime, created_count, bytes)

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
        sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, sensor_name)
        if not sensor_data:
            sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, sensor_name, geom)
        else:
            sensor_id = sensor_data['id']

        stream_list = set([sensor_info.name for sensor_info in netCDF_handle.variables.values() if sensor_info.name.startswith('sensor')])
        for stream in stream_list:
            stream_name = "EnvLog %s - Full Field" % stream
            stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
            if not stream_data:
                stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
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

                        pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, geom,
                                                              time_point, time_point, data_points[index])
            except:
                logging.error("NetCDF attribute not found: %s" % stream)

if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
