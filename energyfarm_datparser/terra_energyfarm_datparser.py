#!/usr/bin/env python

import logging
import os
import requests

from pyclowder.utils import CheckMessage

from pyclowder.files import upload_metadata, download_metadata
from terrautils.extractors import TerrarefExtractor, build_metadata
from terrautils.geostreams import create_datapoint, create_sensor, create_stream, \
	get_stream_by_name, get_sensor_by_name

from parser import *


class MetDATFileParser(TerrarefExtractor):
	def __init__(self):
		super(MetDATFileParser, self).__init__()

		# parse command line and load default logging configuration
		self.setup(sensor='energyfarm_datparser')

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
		self.start_message()

		stream_name = 'Energy Farm Observations'
		disp_name = self.sensors.get_display_name()
		if 'Weather CEN' in resource['name']:
			curr_sens = disp_name + ' - CEN'
			stream_name+= ' CEN'
			main_coords = [-88.199801,40.062051,0]
		elif 'WeatherNE' in resource['name']:
			curr_sens = disp_name + ' - NE'
			stream_name+= ' NE'
			main_coords = [-88.193298,40.067379,0]
		elif 'WeatherSE' in resource['name']:
			curr_sens = disp_name + ' - SE'
			stream_name+= ' SE'
			main_coords = [-88.193573,40.056910,0]
		geom = {
			"type": "Point",
			"coordinates": main_coords
		}

		# Get sensor or create if not found
		sensor_data = get_sensor_by_name(connector, host, secret_key, curr_sens)
		if not sensor_data:
			sensor_id = create_sensor(connector, host, secret_key, curr_sens, geom, {
					"id": "Met Station",
					"title": "Met Station",
					"sensorType": 4
				}, "Urbana")
		else:
			sensor_id = sensor_data['id']

		# Get stream or create if not found
		stream_data = get_stream_by_name(connector, host, secret_key, stream_name)
		if not stream_data:
			stream_id = create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
		else:
			stream_id = stream_data['id']

		# Get metadata to check till what time the file was processed last. Start processing the file after this time
		allmd = download_metadata(connector, host, secret_key, resource['id'])
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

		dp = 0
		for record in records:
			try:
				create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
								record['start_time'], record['end_time'], record['properties'])
				dp += 1
			except:
				logging.error("error creating datapoint at "+record['start_time'])

		# Mark dataset as processed
		metadata = build_metadata(host, self.extractor_info, resource['id'], {
			"last processed time": records[-1]["end_time"],
			"datapoints_created": datapoint_count + dp}, 'file')
		upload_metadata(connector, host, secret_key, resource['id'], metadata)

		self.end_message()

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
