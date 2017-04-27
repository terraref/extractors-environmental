#!/usr/bin/env python

"""
terra_uamac_datparser.py

This extractor will trigger when a file is added to a dataset in Clowder.
It checks if all the required input files are present in the dataset and no metadata
indicating the dataset has already been processed.
If the check is OK, it aggregates all input files into one JSON and inserts it into
GeoStream.
"""

import os
import csv
import json
import requests
import urllib
import urlparse
import logging

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


		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)

		# assign other arguments
		self.sensor_name = self.args.sensor_name
		self.agg_cutoff = self.args.agg_cutoff

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
		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
		main_coords = [ -111.974304, 33.075576, 0]

		# SENSOR is Full Field by default
		sensor_id = get_sensor_id(host, secret_key, self.sensor_name)
		if not sensor_id:
			sensor_id = create_sensor(host, secret_key, self.sensor_name, {
				"type": "Point",
				# These are a point off to the right of the field
				"coordinates": main_coords
			})

		# STREAM is Weather Station
		stream_name = self.sensor_name + " - Weather Observations"
		stream_id = get_stream_id(host, secret_key, stream_name)
		if not stream_id:
			stream_id = create_stream(host, secret_key, sensor_id, stream_name, {
				"type": "Point",
				"coordinates": main_coords
			})

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

			upload_datapoints(host, secret_key, aggregationRecords)
			lastAggregatedFile = file

		# Mark dataset as processed.
		metadata = {
			# TODO: Generate JSON-LD context for additional fields
			"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
			"dataset_id": resource['id'],
			"content": {"status": "COMPLETED"},
			"agent": {
				"@type": "extractor",
				"extractor_id": host + "/api/extractors/" + self.extractor_info['name']
			}
		}
		pyclowder.datasets.upload_metadata(connector, host, secret_key, resource['id'], metadata)

# Get sensor ID from Clowder based on plot name
def get_sensor_id(host, key, name):
	if(not host.endswith("/")):
		host = host+"/"

	url = "%sapi/geostreams/sensors?sensor_name=%s&key=%s" % (host, name, key)
	logging.debug("...searching for sensor : "+name)
	r = requests.get(url)
	if r.status_code == 200:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
	else:
		print("error searching for sensor ID")

	return None

def create_sensor(host, key, name, geom):
	if(not host.endswith("/")):
		host = host+"/"

	body = {
		"name": name,
		"type": "point",
		"geometry": geom,
		"properties": {
			"popupContent": name,
			"type": {
				"id": "MAC Field Scanner",
				"title": "MAC Field Scanner",
				"sensorType": 4
			},
			"name": name,
			"region": "Maricopa"
		}
	}

	url = "%sapi/geostreams/sensors?key=%s" % (host, key)
	logging.info("...creating new sensor: "+name)
	r = requests.post(url,
					  data=json.dumps(body),
					  headers={'Content-type': 'application/json'})
	if r.status_code == 200:
		return r.json()['id']
	else:
		logging.error("error creating sensor")

	return None

# Get stream ID from Clowder based on stream name
def get_stream_id(host, key, name):
	if(not host.endswith("/")):
		host = host+"/"

	url = urlparse.urljoin(host, 'api/geostreams/streams?stream_name=%s&key=%s' % (name, key))
	logging.debug("...searching for stream : "+name)
	r = requests.get(url)
	if r.status_code == 200:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
	else:
		logging.error("error searching for stream ID")

	return None

def create_stream(host, key, sensor_id, name, geom):
	if(not host.endswith("/")):
		host = host+"/"

	body = {
		"name": name,
		"type": "Feature",
		"geometry": geom,
		"properties": {},
		"sensor_id": str(sensor_id)
	}

	url = "%sapi/geostreams/streams?key=%s" % (host, key)
	logging.info("...creating new stream: "+name)
	r = requests.post(url,
					  data=json.dumps(body),
					  headers={'Content-type': 'application/json'})
	if r.status_code == 200:
		return r.json()['id']
	else:
		logging.error("error creating stream: %s" % r.status_code)

	return None

# Save records as JSON back to GeoStream.
def upload_datapoints(host, key, records):
	url = urlparse.urljoin(host, 'api/geostreams/datapoints?key=%s' % key)

	for record in records:
		headers = {'Content-type': 'application/json'}
		r = requests.post(url, data=json.dumps(record), headers=headers)
		if (r.status_code != 200):
			logging.error('Problem creating datapoint : [%s] - %s' % (str(r.status_code), r.text))
	return

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
