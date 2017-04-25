import datetime
import dateutil.tz
import csv
import json


def gallon2mm(value):
    if value:
	#      gallons -> lit = kg -> kg m-2 s-1
        return (int(value)*3.78541)/((20*200)*(24*60*60))
    return 0.0


# Parse CSV file
def parse_file(filepath, main_coords):
    results = []
    GEOMETRY = {
    'type': 'Point',
    'coordinates': main_coords}
    with open(filepath) as csvfile:
        sitename = csvfile.readline().split(',')[1]
        header = []
        while len(header) < 5:
            header.append(csvfile.readline())

        fields = header[-1].split(',')

        reader = csv.DictReader(csvfile, fieldnames=fields)
        utc_offset = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
        for row in reader:

            date = datetime.datetime.strptime(row['Date Time'], '%m/%d/%Y %H:%M').isoformat() + utc_offset.tzname(None)

            record = {
                'start_time': date,
                'properties' : {'irrigation_flux':gallon2mm(row['Gallons'])},
                'type': 'Feature',
                'geometry': GEOMETRY
            }
            results.append(record)
        return results

if __name__ == "__main__":
    infile = "flowmetertotals_March-2017.csv"
    print json.dumps(parse_file(infile)[:5])
