#!/usr/bin/env python

"""
This extractor triggers when a JSON file is added to Clowder.

It calls processing methods in environmental_logger_json2netcdf.py to
convert the JSON file to .nc netCDF format.
"""

import os
import logging
import requests

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files

import environmental_logger_json2netcdf as ela


class EnvironmentLoggerJSON2NetCDF(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def check_message(self, connector, host, secret_key, resource, parameters):
        # Only trigger extraction if the newly added file is a relevant JSON file
        if not resource['name'].endswith("_environmentlogger.json"):
            return False

        return True

    def process_message(self, connector, host, secret_key, resource, parameters):
        # TODO: provide some flag to determine overwrite&replace vs. skip?
        # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
        # fetch metadata from dataset to check if we should remove existing entry for this extractor first
        # md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
        # if len(md) > 0:
        #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)

        in_envlog = resource['local_paths'][0]

        if in_envlog:
            # Execute processing on target file
            timestamp = resource['name'].split("_")[0]
            outputDir = "/home/extractor/sites/ua-mac/Level_1/EnvironmentLogger"
            out_netcdf = os.path.join(outputDir, timestamp, resource['name'][:-5]+".nc")
            if not os.path.exists(os.path.join(outputDir, timestamp)):
                os.makedirs(os.path.join(outputDir, timestamp))
            if not os.path.isfile(out_netcdf):
                print("Converting JSON to: %s" % out_netcdf)
                ela.mainProgramTrigger(in_envlog, out_netcdf)

                # Send netCDF output to Clowder source dataset
                if resource['parent_dataset_id'] == '':
                    # Fetch datasetID by dataset name if not provided
                    dsName = 'EnvironmentLogger - ' + resource['name'].split('_')[0]
                    url = '%s/api/datasets?key=%s&title=%s' % (host, secret_key, dsName)
                    headers={'Content-Type': 'application/json'}

                    r = requests.get(url, headers=headers)
                    if r.status_code == 200:
                        resource['parent_dataset_id'] = r.json()[0]['id']

                pyclowder.files.upload_to_dataset(connector, host, secret_key, resource['parent_dataset_id'], out_netcdf)
            else:
                print("...%s already exists; skipping" % out_netcdf)

if __name__ == "__main__":
    extractor = EnvironmentLoggerJSON2NetCDF()
    extractor.start()
