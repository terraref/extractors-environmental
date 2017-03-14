#!/bin/bash

# test.sh (For environmental_logger pipeline)
#
# This is just a simple shell script for users to test whether the environmental logger
# pipeline works normally (Smoke Testing)

RED='\033[0;31m'
GREEN='\033[0;32m'
END='\033[0m'

has_python=$(module list | grep "python/2.7.10" | wc -m);
has_gdal=$(module list | grep "gdal-stack/2.7.10" | wc -m);

##### Check if the account has Python and gdal-stack (which contains NumPy and netCDF4 and HDF5)
if [ $has_python -eq 0 ];then
    echo -e "${RED}Has no Python, try to load it...${END}"
    module load python/2.7.10
    echo -e "${GREEN}Python loading finished${END}";
    if [ $has_gdal -eq 0 ];then
        echo -e "${RED}Has no gdal-stack, try to load it...${END}"
        module load gdal-stack/2.7.10
        echo -e "${GREEN}gdal-stack loading finished${END}";
    fi
fi

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
eval "${command[@]}" # Run the commands

if [ $? -eq 0 ];then # Check if it returns with 0 (exit Successfully)
    echo -e "${GREEN}Test Run Successfully Exited${END}";
    rm 2016-10-15_19-56-57_environmentlogger.nc; # Clean up
else
    echo -e "${RED}Test Run Failed${END}";
fi
