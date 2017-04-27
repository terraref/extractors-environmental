#!/usr/bin/python

import math
import datetime
import dateutil.parser
import dateutil.tz
import csv
import json

DEBUG = True

def void():
	pass
def log(x):
	print x
debug_log = log if DEBUG else void


def avg(items):
	return float(sum(items)) / max(len(items), 1)

ISO_8601_UTC_MEAN = dateutil.tz.tzoffset(None, 0)

# Convert the given ISO time string to timestamps in seconds.
def ISOTimeString2TimeStamp(timeStr):
	time = dateutil.parser.parse(timeStr)
	isoStartTime = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, ISO_8601_UTC_MEAN)
	return int((time - isoStartTime).total_seconds())

def tempUnit2K(value, unit):
	if unit == 'Deg C':
		return value + 273.15
	elif unit == 'Deg F':
		return (value + 459.67) * 5 / 9
	elif unit == 'Deg K':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)
"""
def relHumidUnit2Percent(value, unit):
	if unit == '%':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)


def speedUnit2MeterPerSecond(value):
	if value == 0:
		return float(nan)
	else:
		value
"""

def extractXFactor(magnitude, degreeFromNorth):
	if(math.isnan(magnitude)):
		return "NaN"
	return magnitude * math.sin(math.radians(degreeFromNorth));
def extractYFactor(magnitude, degreeFromNorth):
	if(math.isnan(magnitude)):
		return "NaN"
	return magnitude * math.cos(math.radians(degreeFromNorth));

STATION_GEOMETRY = {
	'Weather CEN':{
	'type': 'Point',
	'coordinates': [
		# CEN
		40.062051,
		-88.199801,
		0
	]},
	'WeatherNE':{
	'type': 'Point',
	'coordinates': [
		# NE
		40.067379,
		-88.193298,
		0
	]},
	'WeatherSE':{
	'type': 'Point',
	'coordinates': [
		# SE
		40.056910,
		-88.193573,
		0
	]}	
}

# 'AirTC_Avg': 'air_temperature',
# 'RH1_Avg': 'relative_humidity',
#'Pyro': 'surface_downwelling_shortwave_flux_in_air', (not in this data)
# 'PAR_APOGE_Avg': 'surface_downwelling_photosynthetic_photon_flux_in_air',
# 'WindDir_Avg': 'wind_to_direction',
# 'WindSpd_Avg': 'wind_speed',
# 'RAIN_Tot': 'precipitation_rate'
# 'PRESSURE_Avg': 'air_pressue'

#AirTC_Avg","RH1_Avg","WindSpd_Avg","WindSpd_Max","WindDir_Avg","PAR_APOGE_Avg","RAIN_Tot","PRESSURE_Avg"

# Each mapping function can decide to return one or multiple tuple, so leave the list to them.
PROP_MAPPING = {
	'AirTC_Avg': lambda d: [(
		'air_temperature',
		tempUnit2K(float(d['value']), d['meta']['unit'])
	)],
	'RH1_Avg': lambda d: [(
		'relative_humidity',
		float(d['value'])
	)],
	'PAR_APOGE_Avg': lambda d: [(
		'surface_downwelling_photosynthetic_photon_flux_in_air',
		float(d['value'])
	)],
	# If Wind Direction is present, split into speed east and speed north it if we can find Wind Speed.
	'WindDir_Avg': lambda d: [
		('eastward_wind', extractXFactor(float(d['record']['WindDir_Avg']), float(d['value']))),
		('northward_wind', extractYFactor(float(d['record']['WindDir_Avg']), float(d['value'])))
	],
	# If Wind Speed is present, process it if we can find Wind Direction.
	'WindSpd_Avg': lambda d: [(
		'wind_speed',
		float(d['value'])
	)],
	'RAIN_Tot': lambda d: [(
		'precipitation_rate',
		float(d['value'])
	)],
	'PRESSURE_Avg': lambda d:[(
		'air_pressure',
		float(d['value'])
	)]	
}


def transformProps(propMetaDict, propValDict):
	newProps = []
	for propName in propValDict:
		if propName in PROP_MAPPING:
			newProps += PROP_MAPPING[propName]({
				'meta': propMetaDict[propName],
				'value': propValDict[propName],
				'record': propValDict
			})
	return dict(newProps)

def parse_file_header_line(linestr):
	return map(lambda x: json.loads(x), str(linestr).split(','))

# ----------------------------------------------------------------------
# Parse the CSV file and return a list of dictionaries.
def parse_file(filepath, last_processed_time ,utc_offset = ISO_8601_UTC_MEAN):
	results = []
	with open(filepath) as csvfile:
		# First line is always the header.
		# @see {@link https://www.manualslib.com/manual/538296/Campbell-Cr9000.html?page=41#manual}
		header_lines = [
			csvfile.readline()
		]

		file_format, station_name, logger_model, logger_serial, os_version, dld_file, dld_sig, table_name = parse_file_header_line(header_lines[0])

		if file_format != 'TOA5':
			raise ValueError('Unsupported format "%s".' % file_format)

		# For TOA5, there are in total 4 header lines.
		# @see {@link https://www.manualslib.com/manual/538296/Campbell-Cr9000.html?page=43#manual}
		while (len(header_lines) < 4):
			header_lines.append(csvfile.readline())

		prop_names = parse_file_header_line(header_lines[1])
		prop_units = parse_file_header_line(header_lines[2])
		prop_sample_method = parse_file_header_line(header_lines[3])

		# Associate the above lists.
		props = dict()
		for x in xrange(len(prop_names)):
			props[prop_names[x]] = {
				'title': prop_names[x],
				'unit': prop_units[x],
				'sample_method': prop_sample_method[x]
			}
		# [DEBUG] Print the property details if needed.
		#print json.dumps(props)
	
		# move ahead to the last processed time if the file had been processed earlier
		if(last_processed_time!=0):
			timestamp = "0"
			last_time = dateutil.parser.parse(last_processed_time).strftime('%Y-%m-%d %H:%M:%S')
			while(timestamp!=last_time):
				line = csvfile.readline().split(',')
				timestamp = line[0][1:-1]
			timestampPrev = last_processed_time
		else:
			pos = csvfile.tell()
			row = json.loads(csvfile.readline().split(',')[0])
			csvfile.seek(pos)
			timestampPrev = (datetime.datetime.strptime(row, '%Y-%m-%d %H:%M:%S')-datetime.timedelta(minutes=15)).isoformat()+ utc_offset.tzname(None)
			
			
		reader = csv.DictReader(csvfile, fieldnames=prop_names)
 
		
		for row in reader:
			timestamp = datetime.datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S').isoformat() + utc_offset.tzname(None) 		

			newResult = {
				# @type {string}
				'start_time': timestampPrev,
				# @type {string}
				'end_time': timestamp,
				'properties': transformProps(props, row),
				# @type {string}
				'type': 'Feature',
				'geometry': STATION_GEOMETRY[station_name]
			}
			timestampPrev = timestamp
			# Enable this if the raw data needs to be kept.
# 			newResult['properties']['_raw'] = {
# 				'data': row,
# 				'units': prop_units,
# 				'sample_method': prop_sample_method
# 			}
			results.append(newResult)
	return results

if __name__ == "__main__":
	size = 5 * 60
	tz = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
	packages = []

	file = './WeatherSE_Avg15.dat'
	parse = parse_file(file, 0,tz)
	print json.dumps(parse)
