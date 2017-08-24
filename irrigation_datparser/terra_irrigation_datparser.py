#!/usr/bin/env python

import logging
import os

from pyclowder.utils import CheckMessage
from pyclowder.files import upload_metadata
from terrautils.extractors import TerrarefExtractor, build_metadata
from terrautils.geostreams import create_datapoint, create_sensor, create_stream, \
    get_stream_by_name, get_sensor_by_name

from parser import *


class IrrigationFileParser(TerrarefExtractor):
    def __init__(self):
        super(IrrigationFileParser, self).__init__()

        self.setup(sensor='irrigation_datparser')

    def check_message(self, connector, host, secret_key, resource, parameters):
        # TODO: Eventually make this more robust by checking contents
        if resource["name"].startswith("flowmetertotals"):
                return CheckMessage.download

        return CheckMessage.ignore

    def process_message(self, connector, host, secret_key, resource, parameters):
        self.start_message()

        # TODO: Get this from Clowder fixed metadata]
        main_coords = [-111.974304, 33.075576, 361]
        geom = {
            "type": "Point",
            "coordinates": main_coords
        }
        disp_name = self.sensors.get_display_name()

        # Get sensor or create if not found
        sensor_data = get_sensor_by_name(connector, host, secret_key, disp_name)
        if not sensor_data:
            sensor_id = create_sensor(connector, host, secret_key, disp_name, geom, {
                "id": "MAC Met Station",
                "title":"MAC Met Station",
                "sensorType": 4
            }, "Maricopa")
        else:
            sensor_id = sensor_data['id']

        # Get stream or create if not found
        stream_name = "Irrigation Observations"
        stream_data = get_stream_by_name(connector,host, secret_key, stream_name)
        if not stream_data:
            stream_id = create_stream(connector, host, secret_key, stream_name, sensor_id, geom)
        else:
            stream_id = stream_data['id']

        # Process records in file
        records = parse_file(resource["local_paths"][0], main_coords)
        for record in records:
            record['source_file'] = resource["id"]
            record['stream_id'] = str(stream_id)

            create_datapoint(connector, host, secret_key, stream_id, record['geometry'],
                             record['start_time'], record['end_time'], record['properties'])

        # Mark dataset as processed
        metadata = build_metadata(host, self.extractor_info, resource['id'], {
            "datapoints_created": len(records)}, 'file')
        upload_metadata(connector, host, secret_key, resource['id'], metadata)

        self.end_message()

if __name__ == "__main__":
    extractor = IrrigationFileParser()
    extractor.start()
