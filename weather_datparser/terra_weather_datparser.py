#!/usr/bin/env python

import os
import json
import requests
import urlparse
import logging

import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

from parser import *


class MetDATFileParser(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		# add any additional arguments to parser
		# self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
		#                          help='maximum number (default=-1)')
		self.parser.add_argument('--sensor', dest="sensor_name", type=str, nargs='?',
								 default=('AZMET Maricopa Weather Station'),
								 help="sensor name where streams and datapoints should be posted")
		self.parser.add_argument('--aggregation', dest="agg_cutoff", type=int, nargs='?',
								 default=(300),
								 help="minute chunks to aggregate records into (default is 5 mins)")
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
		self.sensor_name = self.args.sensor_name
		self.agg_cutoff = self.args.agg_cutoff
		self.influx_host = self.args.influx_host
		self.influx_port = self.args.influx_port
		self.influx_user = self.args.influx_user
		self.influx_pass = self.args.influx_pass
		self.influx_db = self.args.influx_db

	def check_message(self, connector, host, secret_key, resource, parameters):
		# Check for expected input files before beginning processing
		if len(get_all_files(resource)) >= 23:
			md = pyclowder.datasets.download_metadata(connector, host, secret_key,
													  resource['id'], self.extractor_info['name'])
			for m in md:
				if 'agent' in m and 'name' in m['agent'] and m['agent']['name'].endswith(self.extractor_info['name']):
					logging.info('skipping %s, dataset already handled' % resource['id'])
					return CheckMessage.ignore

			return CheckMessage.download
		else:
			logging.info('skipping %s, not all input files are ready' % resource['id'])
			return CheckMessage.ignore

	def process_message(self, connector, host, secret_key, resource, parameters):
		starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		created_count = 0
		bytes = 0

		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
		main_coords = [ -111.974304, 33.075576, 0]

		# SENSOR is Full Field by default
		sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, self.sensor_name)
		if not sensor_data:
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, self.sensor_name, {
				"type": "Point",
				# These are a point off to the right of the field
				"coordinates": main_coords
			}, {
				"id": "MAC Met Station",
				"title": "MAC Met Station",
				"sensorType": 4
			}, "Maricopa")
		else:
			sensor_id = sensor_data['id']

		# STREAM is Weather Station
		stream_name = self.sensor_name + " - Weather Observations"
		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
				"type": "Point",
				"coordinates": main_coords
			})
		else:
			stream_id = stream_data['id']

		# Find input files in dataset
		target_files = get_all_files(resource)
		datasetUrl = urlparse.urljoin(host, 'datasets/%s' % resource['id'])

		#! Files should be sorted for the aggregation to work.
		aggregationState = None
		lastAggregatedFile = None

		# Process each file and concatenate results together.
		# To work with the aggregation process, add an extra NULL file to indicate we are done with all the files.
		for file in (list(target_files) + [ None ]):
			if file == None:
				# We are done with all the files, finish up aggregation.
				# Pass None as data into the aggregation to let it wrap up any work left.
				records = None
				# The file ID would be the last file processed.
				fileId = lastAggregatedFile['id']
			else:
				# Add this file to the aggregation.
				for p in resource['local_paths']:
					if os.path.basename(p) == file['filename']:
						filepath = p

				# Parse one file and get all the records in it.
				records = parse_file(filepath, utc_offset=ISO_8601_UTC_OFFSET)
				fileId = file['id']

			aggregationResult = aggregate(
					cutoffSize=self.agg_cutoff,
					tz=ISO_8601_UTC_OFFSET,
					inputData=records,
					state=aggregationState
			)
			aggregationState = aggregationResult['state']
			aggregationRecords = aggregationResult['packages']

			# Add props to each record.
			for record in aggregationRecords:
				record['properties']['source'] = datasetUrl
				record['properties']['source_file'] = fileId
				record['stream_id'] = str(stream_id)
				pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
													  record['start_time'], record['end_time'], record['properties'])

			lastAggregatedFile = file

		# Mark dataset as processed.
		metadata = {
			# TODO: Generate JSON-LD context for additional fields
			"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
			"dataset_id": resource['id'],
			"content": {
				"datapoints_created": len(aggregationRecords)
			},
			"agent": {
				"@type": "extractor",
				"extractor_id": host + "/api/extractors/" + self.extractor_info['name']
			}
		}
		pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

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

# Find as many expected files as possible and return the set.
def get_all_files(resource):
	target_files = []

	if 'files' in resource:
		for fileItem in resource['files']:
			fileId   = fileItem['id']
			fileName = fileItem['filename']
			if fileName.endswith(".dat"):
				target_files.append({
					'id': fileId,
					'filename': fileName
				})

	return target_files

def get_output_filename(raw_filename):
	return '%s.nc' % raw_filename[:-len('_raw')]


if __name__ == "__main__":
	extractor = MetDATFileParser()
	extractor.start()
