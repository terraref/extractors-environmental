import logging

import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import pyclowder.geostreams

from parser import *


class IrrigationFileParser(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')
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

        self.setup()

        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

        self.influx_host = self.args.influx_host
        self.influx_port = self.args.influx_port
        self.influx_user = self.args.influx_user
        self.influx_pass = self.args.influx_pass
        self.influx_db = self.args.influx_db

    def check_message(self, connector, host, secret_key, resource, parameters):
        filename = resource["name"]
        # TODO: Eventually make this more robust by checking contents
        if filename.startswith("flowmetertotals"):
                return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        created_count = 0
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

        metadata = {
            "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
            "dataset_id": resource['id'],
            "content": {
                "datapoints_created": len(records)
            },
            "agent": {
                "@type": "extractor",
                "extractor_id": host + "/api/extractors/" + self.extractor_info['name']
            }
        }
        pyclowder.files.upload_metadata(connector, host, secret_key, resource['id'], metadata)

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


if __name__ == "__main__":
    extractor = IrrigationFileParser()
    extractor.start()
