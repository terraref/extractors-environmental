#!/usr/bin/env python

import os
import datetime
import logging
import urlparse

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets
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
								 default=('AZMET Maricopa Weather Station'),
								 help="sensor name where streams and datapoints should be posted")
		self.parser.add_argument('--aggregation', dest="agg_cutoff", type=int, nargs='?',
								 default=(300),
								 help="minute chunks to aggregate records into (default is 5 mins)")
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
		self.agg_cutoff = self.args.agg_cutoff
		self.influx_params = {
			"host": self.args.influx_host,
			"port": self.args.influx_port,
			"db": self.args.influx_db,
			"user": self.args.influx_user,
			"pass": self.args.influx_pass
		}

	def check_message(self, connector, host, secret_key, resource, parameters):
		if not terrautils.extractors.is_latest_file(resource):
			return CheckMessage.ignore

		# Check for expected input files before beginning processing
		if len(get_all_files(resource)) >= 23:
			md = pyclowder.datasets.download_metadata(connector, host, secret_key, resource['id'])
			if terrautils.metadata.get_extractor_metadata(md, self.extractor_info['name']) and not self.force_overwrite:
				logging.info('skipping %s, dataset already handled' % resource['id'])
				return CheckMessage.ignore
			return CheckMessage.download
		else:
			logging.info('skipping %s, not all input files are ready' % resource['id'])
			return CheckMessage.ignore

	def process_message(self, connector, host, secret_key, resource, parameters):
		starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		created = 0
		bytes = 0

		# TODO: Get this from Clowder fixed metadata
		geom = {
			"type": "Point",
			"coordinates": [-111.974304, 33.075576, 0]
		}

		# Get sensor or create if not found
		sensor_data = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, self.sensor_name)
		if not sensor_data:
			sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, self.sensor_name, geom, {
				"id": "MAC Met Station",
				"title": "MAC Met Station",
				"sensorType": 4
			}, "Maricopa")
		else:
			sensor_id = sensor_data['id']

		# Get stream or create if not found
		stream_name = self.sensor_name + " - Weather Observations"
		stream_data = pyclowder.geostreams.get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
		else:
			stream_id = stream_data['id']


		# Process each file and concatenate results together.
		datasetUrl = urlparse.urljoin(host, 'datasets/%s' % resource['id'])
		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
		#! Files should be sorted for the aggregation to work.
		aggregationState = None
		lastAggregatedFile = None
		target_files = get_all_files(resource)
		# To work with the aggregation process, add an extra NULL file to indicate we are done with all the files.
		for file in (list(target_files) + [ None ]):
			if file == None:
				# We are done with all the files, pass None to let aggregation wrap up any work left.
				records = None
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

		# Mark dataset as processed
		metadata = terrautils.extractors.build_metadata(host, self.extractor_info['name'], resource['id'], {
			"datapoints_created": len(aggregationRecords)}, 'dataset')
		pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

		endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
		terrautils.extractors.log_to_influxdb(self.extractor_info['name'], starttime, endtime, created, bytes)

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

if __name__ == "__main__":
	extractor = MetDATFileParser()
	extractor.start()
