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
import pyclowder.geostreams

from parser import *


class IrrigationFileParser(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        self.setup()

        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def check_message(self, connector, host, secret_key, resource, parameters):
	filename = resource["name"]
	if filename.startswith("flowmetertotals"):
        	return CheckMessage.download
	return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):

        #logger = logging.getLogger(__name__)
	main_coords = [-111.974304, 33.075576, 0]
        inputfile = resource["local_paths"][0]
        fileId = resource["id"]

        sensor_name = "AZMET Maricopa Weather Station"
        sensor_id = pyclowder.geostreams.get_sensor_by_name(connector, host, secret_key, sensor_name)
        if not sensor_id:
            sensor_id = pyclowder.geostreams.create_sensor(connector, host, secret_key, sensor_name, {
                "type": "Point",
                "coordinates": main_coords
            }, {
		"id": "Met Station",
		"title":"Met Station",
		"sensorType":4
		}, "Arizona")

        stream_name = "Irrigation Observations"
        stream_id =pyclowder.geostreams.get_stream_by_name(connector,host, secret_key, stream_name)
        if not stream_id:
            stream_id = pyclowder.geostreams.create_stream(connector, host, secret_key, stream_name, sensor_id, {
                "type": "Point",
                "coordinates": main_coords
            })

        records = parse_file(inputfile, main_coords)
        for record in records:
            record['source_file'] = fileId
            record['stream_id'] = str(stream_id)

	for record in records:
		pyclowder.geostreams.create_datapoint(connector, host, secret_key, stream_id, record['geometry'], record['start_time'], record['end_time'], record['properties'])	

        metadata = {
            "@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
            "dataset_id": resource['id'],
            "content": {"status": "COMPLETED"},
            "agent": {
                "@type": "extractor",
                "extractor_id": host + "/api/extractors/" + self.extractor_info['name']
            }
        }

        # logger.debug(metadata)
        pyclowder.files.upload_metadata(connector, host, secret_key, resource['id'], metadata)


if __name__ == "__main__":
    extractor = IrrigationFileParser()
    extractor.start()
