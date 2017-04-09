#!/bin/bash

# test.sh (For environmental_logger pipeline)
#
# This is just a simple shell script for users to test whether the environmental logger
# pipeline works normally (Smoke Testing)

RED='\033[0;31m'
GREEN='\033[0;32m'
END='\033[0m'

##### Check if the account has Python and gdal-stack (which contains NumPy and netCDF4 and HDF5)
[[ $(module list | grep "python/2.7.10" | wc -m) -gt 0 ]] || module load python/2.7.10;
[[ $(module list | grep "gdal-stack/2.7.10" | wc -m) -gt 0 ]] || module load gdal-stack/2.7.10;

# Split the command into an array since it is tooooo looooooong
# --For readability
#
# python <script_name>
# <raw file path>
# <output file path>
#
IFS=' '
read -r -a command <<< $(
tr "\n" " " <<- END
python environmental_logger_json2netcdf.py
/projects/arpae/terraref/sites/ua-mac/raw_data/EnvironmentLogger/2016-10-15/2016-10-15_19-56-57_environmentlogger.json
.
END
)
##### Run the command #####
eval "${command[@]}" 

if [ $? != 0 ]; then
    (>&2 echo -e "${RED}Test Run Failed${END}");
    exit 1;
else
    (>&1 echo -e "${GREEN}Test Run Successfully Exited${END}") && rm 2016-10-15_19-56-57_environmentlogger.nc;
    exit 0;
fi
