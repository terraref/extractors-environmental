import requests
import subprocess
import json

def _push_to_geostream(netCDF_handle, API_key):
    '''
    Try to use ncks (a component of NCO) to get a json dump of netCDF file
    and POST to the Geostream

    *** Need support from bash/tcsh shell and NCO toolkits 4.6.2 and above (and their dependencies) ***
    *** The JSON dump of ncks is developed by Professor Charlie Zender and Mr. Henry Butowsky ***
    '''
    _temporary_file_name = "temporary.json"
    host_template = "https://terraref.ncsa.illinois.edu/clowder/api/geostreams/datapoints?streamid=300&key={}".format(API_key)

    try:
        requests.get(host_template).raise_for_status()
    except HTTPError as err:
        print err
        return -1

    ### Generate the JSON file with ncks ###
    return_code = subprocess.call("ncks --json {} > {}",format(netCDF_handle, _temporary_file_name), shell=True)

    if return_code == 1:
        raise OSError("Invaild netCDF file handle directory")

    with open(_temporary_file_name) as file_handle:
        requests.post(host_template, json=json.dump(json.load(file_handle))) # weird, but using json.dump to make sure that the data is correctly encoded
    
    subprocess.call("rm "+_temporary_file_name) # remove the temporary JSON file
    return 0