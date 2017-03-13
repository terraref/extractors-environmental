import requests
import subprocess
import json
import sys

def _push_to_geostream(netCDF_handle, API_key):
    '''
    Try to use ncks (a component of NCO) to get a json dump of netCDF file
    and POST to the Geostream

    *** Need support from bash/tcsh shell and NCO toolkits 4.6.2 and above (and their dependencies) ***
    *** The JSON dump of ncks is developed by Professor Charlie Zender and Mr. Henry Butowsky ***
    '''
    host_get_template = "https://terraref.ncsa.illinois.edu/clowder/api/geostreams/datapoints?streamid=300&key={}".format(API_key)
    host_post_template = ""

    try:
        requests.get(host_get_template).raise_for_status()
    except HTTPError as err:
        print err
        return -1

    ### Generate the JSON file with ncks ###
    # try:
    raw_JSON = json.loads(subprocess.check_output(["ncks", "--json", netCDF_handle]))
    # except CalledProcessError as err:
    #     print err
    #     return -1

    status = requests.post(host_get_template, json=json.dumps(raw_JSON)) # weird, but using json.dump to make sure that the data is correctly encoded
    status.raise_for_status()
    return 0

    
if __name__ == '__main__':
    _push_to_geostream(sys.argv[1], sys.argv[2])