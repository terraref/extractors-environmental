#!/usr/bin/env python

"""
terra.environmentlogger.py

This extractor will trigger when a file is added to a dataset in Clowder. It checks if the file is an
_environmentlogger.json file, and if so it will call EnvironmentalLoggerAnalyser.py to create a netCDF
output file and add that to the same dataset as the original .json file.
"""

import os
import imp
import logging

from config import *
import pyclowder.extractors as extractors


def main():
    global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints, mountedPaths

    #set logging
    logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
    logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
    logger = logging.getLogger('extractor')
    logger.setLevel(logging.DEBUG)

    # setup
    extractors.setup(extractorName=extractorName,
                     messageType=messageType,
                     rabbitmqURL=rabbitmqURL,
                     rabbitmqExchange=rabbitmqExchange,
                     mountedPaths=mountedPaths)

    # register extractor info
    extractors.register_extractor(registrationEndpoints)

    #connect to rabbitmq
    extractors.connect_message_bus(extractorName=extractorName,
                                   messageType=messageType,
                                   processFileFunction=process_dataset,
                                   checkMessageFunction=check_message,
                                   rabbitmqExchange=rabbitmqExchange,
                                   rabbitmqURL=rabbitmqURL)

# ----------------------------------------------------------------------
def check_message(parameters):
    # Only trigger extraction if the newly added file is a relevant JSON file
    if not parameters['filename'].endswith("_environmentlogger.json"):
        return False

    # For now if the dataset already has metadata from this extractor, don't recreate
    md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    if len(md) > 0:
        for m in md:
            if 'agent' in m and 'name' in m['agent']:
                if m['agent']['name'].find(extractorName) > -1:
                    print("skipping dataset %s, already processed" % parameters['datasetId'])
                    return False

    return True


def process_dataset(parameters):
    global outputDir

    # TODO: re-enable once this is merged into Clowder: https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/clowder/pull-requests/883/overview
    # fetch metadata from dataset to check if we should remove existing entry for this extractor first
    # md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
    # if len(md) > 0:
        #extractors.remove_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)

    in_envlog = parameters['filepath']

    if in_envlog:
        # Execute processing on target file
        timestamp = parameters['filename'].split("_")[0]
        out_netcdf = os.path.join(outputDir, timestamp, parameters['filename'][:-5]+".nc")
        print("Converting JSON to: %s" % out_netcdf)
        if not os.path.exists(os.path.join(outputDir, timestamp)):
            os.makedirs(os.path.join(outputDir, timestamp))
        ela.mainProgramTrigger(in_envlog, out_netcdf)

        # Send netCDF output to Clowder source dataset
        extractors.upload_file_to_dataset(out_netcdf, parameters)

if __name__ == "__main__":
    global scriptPath

    # Import EnvironmentalLoggerAnalyser script from configured location
    ela = imp.load_source('environmental_logger_json2netcdf', scriptPath)

    main()
