#!/usr/bin/env python

import requests
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


class MetDATFileParser(Extractor):
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

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		# assign other arguments
		self.influx_host = self.args.influx_host
		self.influx_port = self.args.influx_port
		self.influx_user = self.args.influx_user
		self.influx_pass = self.args.influx_pass
		self.influx_db = self.args.influx_db

	def check_message(self, connector, host, secret_key, resource, parameters):
		# Weather CEN_Avg15.dat, Weather CEN_DayAvg.dat
		# WeatherNE_Avg15.dat,   WeatherNE_DayAvg.dat
		# WeatherSE_Avg15.dat,   WeatherSE_DayAvg.dat
		#   For now only handle the _Avg15 datasets.
		target_files = ['CEN', 'NE', 'SE']
		filename = resource['name']

		for target_part in target_files:
			if filename.startswith('Weather') and filename.endswith("_Avg15.dat") and target_part in filename:
				return CheckMessage.download

		# If we didn't match the desired filenames, skip this .dat file
		return CheckMessage.ignore

	def process_message(self, connector, host, secret_key, resource, parameters):
		starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		created_count = 0
		bytes = 0

		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
	
		# Get input files
		logger = logging.getLogger(__name__)
		inputfile = resource["local_paths"][0]
		fileId = resource['id']
		filename = resource['name']

		sensor_name = 'UIUC Energy Farm - '
		stream_name = 'Energy Farm Observations '

		if 'CEN' in filename:
			sensor_name+= 'CEN'
			stream_name+= 'CEN'
			main_coords = [-88.199801,40.062051,0]
		elif 'NE' in filename:
			sensor_name+= 'NE'
			stream_name+= 'NE'
			main_coords = [-88.193298,40.067379,0]
		elif 'SE' in filename:
			sensor_name+= 'SE'
			stream_name+= 'SE'
			main_coords = [-88.193573,40.056910,0]

		sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, sensor_name)
		if not sensor_data:
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, sensor_name, {
					"type": "Point",
					# These are a point off to the right of the field
					"coordinates": main_coords
				}, {
					"id": "Met Station",
					"title": "Met Station",
					"sensorType": 4
				}, "Urbana")
		else:
			sensor_id = sensor_data['id']

		# Look for stream.
		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
					"type": "Point",
					"coordinates": [0,0,0]
				})
		else:
			stream_id = stream_data['id']
		
		# Get metadata to check till what time the file was processed last. Start processing the file after this time
		allmd = pyclowder.files.download_metadata(connector, host, secret_key, resource['id'])
		last_processed_time = 0
		datapoint_count = 0
		for md in allmd:
			if 'content' in md and 'last processed time' in md['content']:
				last_processed_time = md['content']['last processed time']
				if 'datapoints_created' in md['content']:
					datapoint_count = md['content']['datapoints_created']
				else:
					datapoint_count = 0
				delete_metadata(connector, host, secret_key, resource['id'], md['agent']['name'].split("/")[-1])

		# Parse file and get all the records in it.
		records = parse_file(inputfile, last_processed_time, utc_offset=ISO_8601_UTC_OFFSET)
		# Add props to each record.
		for record in records:
			record['properties']['source_file'] = fileId
			record['stream_id'] = str(stream_id)

		for record in records:
			pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
												  record['start_time'], record['end_time'], record['properties'])

		metadata = {
			"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
			"dataset_id": resource['id'],
			"content": {
				"last processed time": records[-1]["end_time"],
				"datapoints_created": datapoint_count + len(records)
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


def delete_metadata(connector, host, key, fileid, extractor=None):
    """Delete file JSON-LD metadata from Clowder.
    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """
    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%sapi/files/%s/metadata.jsonld?key=%s%s' % (host, fileid, key, filterstring)
    # fetch data
    result = requests.delete(url, stream=True,
                          verify=connector.ssl_verify)
    result.raise_for_status()
    return result.json()


if __name__ == "__main__":
	extractor = MetDATFileParser()
	extractor.start()
