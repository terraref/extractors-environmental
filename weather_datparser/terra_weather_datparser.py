#!/usr/bin/env python

import os
import logging
import urlparse

from pyclowder.utils import CheckMessage
from pyclowder.datasets import download_metadata, upload_metadata
from pyclowder.metadata import get_extractor_metadata
from terrautils.extractors import TerrarefExtractor, is_latest_file, build_metadata
from terrautils.geostreams import create_datapoint, create_sensor, create_stream, \
	get_stream_by_name, get_sensor_by_name

from parser import *


def add_local_arguments(parser):
	# add any additional arguments to parser
	parser.add_argument('--aggregation', dest="agg_cutoff", type=int, nargs='?',
					default=(300),
					help="minute chunks to aggregate records into (default is 5 mins)")

class MetDATFileParser(TerrarefExtractor):
	def __init__(self):
		super(MetDATFileParser, self).__init__()

		# add any additional arguments to parser
		add_local_arguments(self.parser)

		# parse command line and load default logging configuration
		self.setup(sensor='weather_datparser')

		# assign other arguments
		self.agg_cutoff = self.args.agg_cutoff

	def check_message(self, connector, host, secret_key, resource, parameters):
		if not is_latest_file(resource):
			return CheckMessage.ignore

		# Check for expected input files before beginning processing
		if len(get_all_files(resource)) >= 23:
			md = download_metadata(connector, host, secret_key, resource['id'])
			if get_extractor_metadata(md, self.extractor_info['name']) and not self.overwrite:
				logging.info('skipping %s, dataset already handled' % resource['id'])
				return CheckMessage.ignore
			return CheckMessage.download
		else:
			logging.info('skipping %s, not all input files are ready' % resource['id'])
			return CheckMessage.ignore

	def process_message(self, connector, host, secret_key, resource, parameters):
		self.start_message()

		# TODO: Get this from Clowder fixed metadata
		geom = {
			"type": "Point",
			"coordinates": [-111.974304, 33.075576, 361]
		}
		disp_name = self.sensors.get_display_name()

		# Get sensor or create if not found
		sensor_data = get_sensor_by_name(connector, host, secret_key, disp_name)
		if not sensor_data:
			sensor_id = create_sensor(connector, host, secret_key, disp_name, geom, {
				"id": "MAC Met Station",
				"title": "MAC Met Station",
				"sensorType": 4
			}, "Maricopa")
		else:
			sensor_id = sensor_data['id']

		# Get stream or create if not found
		stream_name = "Weather Observations"
		stream_data = get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
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
				create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
								record['start_time'], record['end_time'], record['properties'])

			lastAggregatedFile = file

		# Mark dataset as processed
		metadata = build_metadata(host, self.extractor_info, resource['id'], {
			"datapoints_created": len(aggregationRecords)}, 'dataset')
		upload_metadata(connector, host, secret_key, resource['id'], metadata)

		self.end_message()

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
