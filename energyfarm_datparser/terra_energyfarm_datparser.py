#!/usr/bin/env python

import datetime
import logging
import os
import requests

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
import pyclowder.geostreams
import terrautils.extractors

from parser import *


class MetDATFileParser(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		influx_host = os.getenv("INFLUXDB_HOST", "terra-logging.ncsa.illinois.edu")
		influx_port = os.getenv("INFLUXDB_PORT", 8086)
		influx_db = os.getenv("INFLUXDB_DB", "extractor_db")
		influx_user = os.getenv("INFLUXDB_USER", "terra")
		influx_pass = os.getenv("INFLUXDB_PASSWORD", "")

		# add any additional arguments to parser
		self.parser.add_argument('--sensor', dest="sensor_name", type=str, nargs='?',
								 default=('UIUC Energy Farm'),
								 help="sensor name where streams and datapoints should be posted")
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
		self.sensor_name = self.args.sensor_name
		self.influx_params = {
			"host": self.args.influx_host,
			"port": self.args.influx_port,
			"db": self.args.influx_db,
			"user": self.args.influx_user,
			"pass": self.args.influx_pass
		}

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
		created = 0
		bytes = 0

		# TODO: Replace these with calls to Clowder fixed metadata
		stream_name = 'Energy Farm Observations'
		if 'CEN' in resource['name']:
			self.sensor_name+= ' - CEN'
			stream_name+= ' CEN'
			main_coords = [-88.199801,40.062051,0]
		elif 'NE' in resource['name']:
			self.sensor_name+= ' - NE'
			stream_name+= ' NE'
			main_coords = [-88.193298,40.067379,0]
		elif 'SE' in resource['name']:
			self.sensor_name+= ' - SE'
			stream_name+= ' SE'
			main_coords = [-88.193573,40.056910,0]
		geom = {
			"type": "Point",
			"coordinates": main_coords
		}

		# Get sensor or create if not found
		sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, self.sensor_name)
		if not sensor_data:
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, self.sensor_name, geom, {
					"id": "Met Station",
					"title": "Met Station",
					"sensorType": 4
				}, "Urbana")
		else:
			sensor_id = sensor_data['id']

		# Get stream or create if not found
		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
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
		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
		records = parse_file(resource["local_paths"][0], last_processed_time, utc_offset=ISO_8601_UTC_OFFSET)
		# Add props to each record.
		for record in records:
			record['properties']['source_file'] = resource['id']
			record['stream_id'] = str(stream_id)

		for record in records:
			pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
												  record['start_time'], record['end_time'], record['properties'])

		# Mark dataset as processed
		metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
			"last processed time": records[-1]["end_time"],
			"datapoints_created": datapoint_count + len(records)}, 'file')
		pyclowder.files.upload_metadata(connector, host, secret_key, resource['id'], metadata)

		endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		terrautils.extractors.log_to_influxdb(self.extractor_info['name'], starttime, endtime, created, bytes)

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