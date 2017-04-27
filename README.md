# Environmental extractors

This repository contains extractors that process data originating from:
- GMP 343 CO2 sensor
- Thies Clima environmental sensors
- Maricopa lightning/irrigation/weather data


### Environmental Logger JSON 2 NetCDF extractor
This extractor processes environmental logger stream data .JSON files into a netCDF 

_Input_

  - Evaluation is triggered whenever a file is added to a dataset
  - Checks whether the file is an _environmentlogger.json file
  
_Output_

  - The dataset containing the .JSON file will get a corresponding .nc netCDF file

### UAMAC/UIUC Energy Farm DAT parser extractors
This extractor extracts metadata from meteorological DAT files into netCDF, as well as creating entries in the Clowder Geostreams database.

_Input_

  - Evaluation is triggered whenever 24 .dat files are added to a dataset
  			
_Output_

  - netCDF metadata is generated and added to dataset
  - datapoints for each record in the DAT files are added to geostream
  