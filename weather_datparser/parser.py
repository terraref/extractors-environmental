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

def relHumidUnit2Percent(value, unit):
	if unit == '%':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)

def speedUnit2MeterPerSecond(value, unit):
	if unit == 'meters/second':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)

def extractXFactor(magnitude, degreeFromNorth):
	return magnitude * math.sin(math.radians(degreeFromNorth));
def extractYFactor(magnitude, degreeFromNorth):
	return magnitude * math.cos(math.radians(degreeFromNorth));

STATION_GEOMETRY = {
	'type': 'Point',
	'coordinates': [
		# SW Corner.
		# @see {@link https://github.com/terraref/extractors-metadata/blob/master/sensorposition/terra.sensorposition.py#L68}
		33.0745666667,
		-111.9750833333,
		0
	]
}

# 'AirTC': 'air_temperature',
# 'RH': 'relative_humidity',
# 'Pyro': 'surface_downwelling_shortwave_flux_in_air',
# 'PAR_ref': 'surface_downwelling_photosynthetic_photon_flux_in_air',
# 'WindDir': 'wind_to_direction',
# 'WS_ms': 'wind_speed',
# 'Rain_mm_Tot': 'precipitation_rate'

# Each mapping function can decide to return one or multiple tuple, so leave the list to them.
PROP_MAPPING = {
	'AirTC': lambda d: [(
		'air_temperature',
		tempUnit2K(float(d['value']), d['meta']['unit'])
	)],
	'RH': lambda d: [(
		'relative_humidity',
		relHumidUnit2Percent(float(d['value']), d['meta']['unit'])
	)],
	'Pyro': lambda d: [(
		'surface_downwelling_shortwave_flux_in_air',
		float(d['value'])
	)],
	'PAR_ref': lambda d: [(
		'surface_downwelling_photosynthetic_photon_flux_in_air',
		float(d['value'])
	)],
	# If Wind Direction is present, split into speed east and speed north it if we can find Wind Speed.
	'WindDir': lambda d: [
		('eastward_wind', extractXFactor(float(d['record']['WS_ms']), float(d['value']))),
		('northward_wind', extractYFactor(float(d['record']['WS_ms']), float(d['value'])))
	],
	# If Wind Speed is present, process it if we can find Wind Direction.
	'WS_ms': lambda d: [(
		'wind_speed',
		speedUnit2MeterPerSecond(float(d['value']), d['meta']['unit'])
	)],
	'Rain_mm_Tot': lambda d: [(
		'precipitation_rate',
		float(d['value'])
	)]
}

# Aggregation functions for each property.
PROP_AGGREGATE = {
	'air_temperature': avg,
	'relative_humidity': avg,
	'surface_downwelling_shortwave_flux_in_air': avg,
	'surface_downwelling_photosynthetic_photon_flux_in_air': avg,
	'eastward_wind': avg,
	'northward_wind': avg,
	'wind_speed': avg,
	'precipitation_rate': sum
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
def parse_file(filepath, utc_offset = ISO_8601_UTC_MEAN):
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

		reader = csv.DictReader(csvfile, fieldnames=prop_names)
		for row in reader:
			timestamp = datetime.datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S').isoformat() + utc_offset.tzname(None)
			newResult = {
				# @type {string}
				'start_time': timestamp,
				# @type {string}
				'end_time': timestamp,
				'properties': transformProps(props, row),
				# @type {string}
				'type': 'Feature',
				'geometry': STATION_GEOMETRY
			}
			# Enable this if the raw data needs to be kept.
# 			newResult['properties']['_raw'] = {
# 				'data': row,
# 				'units': prop_units,
# 				'sample_method': prop_sample_method
# 			}
			results.append(newResult)
	return results

# ----------------------------------------------------------------------
# Aggregate the list of parsed results.
# The aggregation starts with the input data and no state given.
# This function returns a list of aggregated data packages and a state package
# which should be fed back into the function to continue or end the aggregation.
# If there's no more data to input, provide None and the aggregation will stop.
# When aggregation ended, the state package returned should be None to indicate that.
# Note: data has to be sorted by time.
# Note: cutoffSize is in seconds.
def aggregate(cutoffSize, tz, inputData, state):
	# This function should always return this complex package no matter what happens.
	result = {
		'packages': [],
		# In case the input data does nothing, inherit the state first.
		'state': None if state == None else dict(state)
	}

	# The aggregation ends when no more data is available. (inputData is None)
	# In which case it needs to recover leftover data in the state package.
	if inputData == None:
		debug_log('Ending aggregation...')

		# The aggregation is ending, try recover leftover data from the state.
		if state == None:
			# There is nothing to do.
			pass
		else:
			# Recover leftover data from state.

			data = state['leftover']

			if len(data) == 0:
				# There is nothing to recover.
				pass
			else:
				# Aggregate leftover data.
				# Assume leftover data never contain more data than the cutoff allows.

				startTime = state['starttime']
				# Use the latest date in the data entries.
				# Assuming the data is always sorted, the last one should be the latest.
				endTime = ISOTimeString2TimeStamp(data[-1]['end_time'])

				newPackage = aggregate_chunk(data, tz, startTime, endTime)
				if newPackage != None:
					result['packages'].append(newPackage)

			# Mark state with None to indicate the aggregation is done.
			result['state'] = None
	else:
		debug_log('Aggregating...')

		data = inputData

		# More data is provided, continue aggregation.
		if state == None:
			debug_log('Fresh start...')
			# There is no previous state, starting afresh.

			# Use the earliest date in the input data entries.
			# Assuming the input data is always sorted, the first one should be the earliest.
			startTime = ISOTimeString2TimeStamp(data[0]['start_time'])

		else:
			debug_log('Continuing...')
			# Resume aggregation from a previous state.

			startTime = state['starttime']
			# Left over data should be part of the data being processed.
			data = state['leftover'] + inputData

		startIndex = 0

		# Keep aggregating until all the data is consumed.
		while startIndex < len(data):
			# Find the nearest cut-off point.
			endTimeCutoff = startTime - startTime % cutoffSize + cutoffSize
			# Scan the input data to find the portion that fits in the cutoff.
			endIndex = startIndex
			while endIndex < len(data) and ISOTimeString2TimeStamp(data[endIndex]['end_time']) < endTimeCutoff:
				endIndex += 1

			# If everything fits in the cutoff, there may be more data in the next run.
			# Otherwise, these data should be aggregated.
			if endIndex >= len(data):
				# End of data reached, but cutoff is not.
				# Save everything into state.
				result['state'] = {
					'starttime': startTime,
					'leftover': data[startIndex:]
				}
			else:
				# Cutoff reached.
				# Aggregate this chunk.
				newPackage = aggregate_chunk(data[startIndex:endIndex], tz, startTime, endTimeCutoff)
				if newPackage != None:
					result['packages'].append(newPackage)

			# Update variables for the next loop.
			startTime = endTimeCutoff
			startIndex = endIndex

		# The above loop should end with some chunks aggregated into result['packages'],
		# with the last chunk saved in result['state']['leftover']

	return result

# Helper function for aggregating a chunk of data.
# @param {timestamp} startTime
# @param {timestamp} endTime
def aggregate_chunk(dataChunk, tz, startTime, endTime):
	if len(dataChunk) == 0:
		# There is nothing to aggregate.
		return None
	else:
		# Prepare the list of properties for aggregation.
		propertiesList = map(lambda x: x['properties'], dataChunk)

		return {
			'start_time': datetime.datetime.fromtimestamp(startTime, tz).isoformat(),
			'end_time': datetime.datetime.fromtimestamp(endTime, tz).isoformat(),
			'properties': aggregateProps(propertiesList),
			'type': 'Point',
			'geometry': STATION_GEOMETRY
		}

def aggregateProps(propertiesList):
	collection = {}

	for properties in propertiesList:
		for key in properties:
			# Properties start with "_" shouldn't be processed.
			if key.startswith('_'):
				continue
			value = properties[key]
			# Collect property values and save them into collection
			if key not in collection:
				collection[key] = [value]
			else:
				collection[key].append(value)

	result = {}
	for key in properties:
		# If there is no aggregation function, ignore the property.
		if key not in PROP_AGGREGATE:
			continue
		func = PROP_AGGREGATE[key]
		result[key] = func(collection[key])

	return result

if __name__ == "__main__":
	size = 5 * 60
	tz = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
	packages = []

	file = './test-input-1.dat'
	parse = parse_file(file, tz)
	result = aggregate(
		cutoffSize=size,
		tz=tz,
		inputData=parse,
		state=None
	)
	packages += result['packages']
	print json.dumps(result['state'])

	file = './test-input-2.dat'
	parse = parse_file(file, tz)
	result = aggregate(
		cutoffSize=size,
		tz=tz,
		inputData=parse,
		state=result['state']
	)
	packages += result['packages']
	print json.dumps(result['state'])

	result = aggregate(
		cutoffSize=size,
		tz=tz,
		inputData=None,
		state=result['state']
	)
	packages += result['packages']
	print 'Package Count: %s' % (len(packages))

	packages = []

	file = './test-input-3.dat'
	parse = parse_file(file, tz)
	result = aggregate(
		cutoffSize=size,
		tz=tz,
		inputData=parse,
		state=result['state']
	)
	packages += result['packages']

	result = aggregate(
		cutoffSize=size,
		tz=tz,
		inputData=None,
		state=result['state']
	)
	packages += result['packages']
	print 'Package Count: %s' % (len(packages))
	print json.dumps(packages)
