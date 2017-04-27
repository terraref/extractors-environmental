#!/usr/bin/env python

import os
import logging
import requests

import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import pyclowder.geostreams

from netCDF4  import Dataset
from datetime import datetime, timedelta

import environmental_logger_json2netcdf as ela


class EnvironmentLoggerJSON2NetCDF(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
        self.parser.add_argument('--output', '-o', dest="output_dir", type=str, nargs='?',
                                 default="/home/extractor/sites/ua-mac/Level_1/EnvironmentLogger",
                                 help="root directory where timestamp & output directories will be created")
        self.parser.add_argument('--overwrite', dest="force_overwrite", type=bool, nargs='?', default=False,
                                 help="whether to overwrite output file if it already exists in output directory")
        self.parser.add_argument('--influxHost', dest="influx_host", type=str, nargs='?',
                                 default="terra-logging.ncsa.illinois.edu", help="InfluxDB URL for logging")
        self.parser.add_argument('--influxPort', dest="influx_port", type=int, nargs='?',
                                 default=8086, help="InfluxDB port")
        self.parser.add_argument('--influxUser', dest="influx_user", type=str, nargs='?',
                                 default="terra", help="InfluxDB username")
        self.parser.add_argument('--influxPass', dest="influx_pass", type=str, nargs='?',
                                 default="", help="InfluxDB password")
        self.parser.add_argument('--influxDB', dest="influx_db", type=str, nargs='?',
                                 default="extractor_db", help="InfluxDB databast")

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        # assign other arguments
        self.output_dir = self.args.output_dir
        self.force_overwrite = self.args.force_overwrite
        self.influx_host = self.args.influx_host
        self.influx_port = self.args.influx_port
        self.influx_user = self.args.influx_user
        self.influx_pass = self.args.influx_pass
        self.influx_db = self.args.influx_db

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

        if in_envlog:
            # Prepare output directory path - "output_dir/YYYY-MM-DD/filename.nc"
            timestamp = resource['name'].split("_")[0]
            out_netcdf = os.path.join(self.output_dir, timestamp, resource['name'][:-5]+".nc")
            if not os.path.exists(os.path.join(self.output_dir, timestamp)):
                os.makedirs(os.path.join(self.output_dir, timestamp))

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

                if 'parent' in resource and resource['parent']['id'] != '':
                    ds_files = pyclowder.datasets.get_file_list(connector, host, secret_key, resource['parent']['id'])
                    found_output_in_dataset = False
                    for f in ds_files:
                        if f['filename'] == resource['name'][:-5]+".nc":
                            found_output_in_dataset = True
                    if not found_output_in_dataset:
                        logging.info("uploading netCDF file to Clowder")
                        pyclowder.files.upload_to_dataset(connector, host, secret_key,
                                                      resource['parent']['id'], out_netcdf)
                    else:
                        logging.info("%s already in Clowder; not re-uploading" % out_netcdf)
                else:
                    logging.error('no parent dataset ID found; unable to upload to Clowder')
                    raise Exception('no parent dataset ID found')

                # Push to geostreams
                prepareDatapoint(connector, host, secret_key, resource, out_netcdf)

            else:
                logging.info("%s already exists; skipping" % out_netcdf)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.logToInfluxDB(starttime, endtime, created_count, bytes)

    def logToInfluxDB(self, starttime, endtime, filecount, bytecount):
        # Time of the format "2017-02-10T16:09:57+00:00"
        f_completed_ts = int(parse(endtime).strftime('%s'))
        f_duration = f_completed_ts - int(parse(starttime).strftime('%s'))

        client = InfluxDBClient(self.influx_host, self.influx_port, self.influx_user, self.influx_pass, self.influx_db)
        client.write_points([{
            "measurement": "file_processed",
            "time": f_completed_ts,
            "fields": {"value": f_duration}
        }], tags={"extractor": self.extractor_info['name'], "type": "duration"})
        client.write_points([{
            "measurement": "file_processed",
            "time": f_completed_ts,
            "fields": {"value": int(filecount)}
        }], tags={"extractor": self.extractor_info['name'], "type": "filecount"})
        client.write_points([{
            "measurement": "file_processed",
            "time": f_completed_ts,
            "fields": {"value": int(bytecount)}
        }], tags={"extractor": self.extractor_info['name'], "type": "bytes"})

def _produce_attr_dict(netCDF_variable_obj):
    '''
    Produce a list of dictionary with attributes and value (Each dictionary is one datapoint)
    '''
    attributes = [attr for attr in dir(netCDF_variable_obj) if isinstance(attr, unicode)]
    result     = {name:getattr(netCDF_variable_obj, name) for name in attributes}

    return [dict(result.items()+ {"value":str(data)}.items()) for data in netCDF_variable_obj[...]]

def prepareDatapoint(connector, host, secret_key, resource, ncdf):
    coords = [-111.974304, 33.075576, 0]

    with Dataset(ncdf, "r") as netCDF_handle:
        sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, "Full Field - Environmental Logger")
        if not sensor_data:
            sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, "Full Field - Environmental Logger", {
                "type": "Point",
                "coordinates": coords
            })
        else:
            sensor_id = sensor_data['id']

        stream_list = set([getattr(data, u'sensor') for data in netCDF_handle.variables.values() if u'sensor' in dir(data)])
        for stream in stream_list:
            # STREAM is plot x instrument
            stream_name = "EnvLog %s - Full Field" % stream
            logging.debug("checking for stream %s" % stream_name)
            stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
            if not stream_data:
                logging.debug("...stream not found. creating")
                stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
                    "type": "Point",
                    "coordinates": coords
                })
            else:
                stream_id = stream_data['id']

            try:
                memberlist = netCDF_handle.get_variables_by_attributes(sensor=stream)
                for members in memberlist:
                    data_points = _produce_attr_dict(members)

                    for index in range(len(data_points)):
                        time_format = "%Y-%m-%dT%H:%M:%s-07:00"
                        time_point = (datetime(year=1970, month=1, day=1) + \
                                      timedelta(days=netCDF_handle.variables["time"][index])).strftime(time_format)

                        pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, {
                            "type": "Point",
                            "coordinates": coords
                        }, time_point, time_point, data_points[index])
            except:
                logging.error("NetCDF attribute not found: %s" % stream)


if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
