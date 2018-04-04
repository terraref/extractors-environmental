import datetime
import dateutil.tz
import csv
import json


def gallon2mm(value):
    # gallons -> lit = kg -> kg m-2 s-1
    if value:
        return (int(value)*3.78541)/((20*200)*(24*60*60))
    return 0.0

# Parse CSV file
def parse_file(filepath, main_coords):
    results = []

    with open(filepath) as csvfile:
        header = []
        found_date = False
        while (not found_date) or len(header)<5:
            curr_line = csvfile.readline()
            header.append(curr_line)
            if curr_line.find("Date Time") > -1:
                found_date = True
        fields = header[-1].split(',')

        reader = csv.DictReader(csvfile, fieldnames=fields)
        utc_offset = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)

        for row in reader:
            try:
                start_time = datetime.datetime.strptime(row['Date Time'], '%m/%d/%Y %H:%M').isoformat() + utc_offset.tzname(None)
                end_time = (datetime.datetime.strptime(row['Date Time'], '%m/%d/%Y %H:%M')+datetime.timedelta(0,0,0,0,59,23)).isoformat() + utc_offset.tzname(None)
            except:
                continue

            if 'Actual' in row and row['Actual'] != '':
                results.append({
                    'start_time': start_time,
                    'end_time': end_time,
                    'properties' : {'irrigation_flux':gallon2mm(row['Actual'])},
                    'type': 'Feature',
                    'geometry': {
                        'type': 'Point',
                        'coordinates': main_coords
                    }
                })

        return results


if __name__ == "__main__":
    infile = "flowmetertotals_March-2017.csv"
    print json.dumps(parse_file(infile)[:5])
