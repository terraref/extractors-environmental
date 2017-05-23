#!/usr/bin/env python

import datetime
import logging

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import pyclowder.geostreams
import terrautils.extractors

from parser import *


class IrrigationFileParser(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
        influx_port = os.getenv("INFLUXDB_PORT", 8086)
        influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
        influx_user = os.getenv("INFLUXDB_USER", "terra")
        influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
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

        self.setup()

        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        self.influx_params = {
            "host": self.args.influx_host,
            "port": self.args.influx_port,
            "db": self.args.influx_db,
            "user": self.args.influx_user,
            "pass": self.args.influx_pass
        }

    def check_message(self, connector, host, secret_key, resource, parameters):
        filename = resource["name"]
        # TODO: Eventually make this more robust by checking contents
        if filename.startswith("flowmetertotals"):
                return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created = 0
        bytes = 0

        main_coords = [-111.974304, 33.075576, 361]
        inputfile = resource["local_paths"][0]
        fileId = resource["id"]

        sensor_name = "AZMET Maricopa Weather Station"
        sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, sensor_name)
        if not sensor_data:
            sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, sensor_name, {
                "type": "Point",
                "coordinates": main_coords
            }, {
                "id": "MAC Met Station",
                "title":"MAC Met Station",
                "sensorType": 4
            }, "Maricopa")
        else:
            sensor_id = sensor_data['id']

        stream_name = "Irrigation Observations"
        stream_data =pyclowder.geostreams.get_stream_by_name(connector,host, secret_key, stream_name)
        if not stream_data:
            stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
                "type": "Point",
                "coordinates": main_coords
            })
        else:
            stream_id = stream_data['id']

        records = parse_file(inputfile, main_coords)
        for record in records:
            record['source_file'] = fileId
            record['stream_id'] = str(stream_id)

            pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
                                                  record['start_time'], record['end_time'], record['properties'])

        metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
            "datapoints_created": len(records)}, 'file')
        pyclowder.files.upload_metadata(connector, host, secret_key, resource['id'], metadata)

        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        terrautils.extractors.log_to_influxdb(self.extractor_info['name'], starttime, endtime, created, bytes)

if __name__ == "__main__":
    extractor = IrrigationFileParser()
    extractor.start()
