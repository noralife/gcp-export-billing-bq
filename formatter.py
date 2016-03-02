import csv
from dateutil.parser import parse
import json
import time


# before: 2016-02-26T00:00:00-08:00
#  after: 2016-02-26 08:00:00
def isodate2bgdate(isodate):
    dt = parse(isodate)
    unixtime = int(time.mktime(dt.utctimetuple()))
    bgdate = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(unixtime))
    return bgdate


# before: Start Time
#  after: start_time
def format_rows(rows):
    formatted_rows = []
    for row in rows:
        formatted_row = {}
        for k, v in row.items():
            k = k.replace(" ", "_").lower()
            formatted_row[k] = v
        formatted_rows.append(formatted_row)
    return formatted_rows


def load_export_billing_csv(reader):
    rows = []
    for row in reader:
        if len(row['Project']) > 0:
            row['Project'] = int(row['Project'])
        else:
            row['Project'] = None
        if len(row['Measurement1 Total Consumption']) > 0:
            row['Measurement1 Total Consumption'] = int(row['Measurement1 Total Consumption'])
        else:
            row['Measurement1 Total Consumption'] = None
        if len(row['Credit1 Amount']) > 0:
            row['Credit1 Amount'] = float(row['Credit1 Amount'])
        else:
            row['Credit1 Amount'] = None
        if len(row['Cost']) > 0:
            row['Cost'] = float(row['Cost'])
        else:
            row['Cost'] = None
        if len(row['Project Number']) > 0:
            row['Project Number'] = int(row['Project Number'])
        else:
            row['Project Number'] = None
        row['End Time'] = isodate2bgdate(row['End Time'])
        row['Start Time'] = isodate2bgdate(row['Start Time'])
        rows.append(row)
    return rows


def load_export_billing_csv_from_file(fname):
    with open(fname, 'r') as f:
        reader = csv.DictReader(f)
        return load_export_billing_csv(reader)


def load_export_billing_csv_from_memory(data):
    reader = csv.DictReader(data.strip().splitlines())
    return load_export_billing_csv(reader)


# save bigquery style json file
def export_json_with_new_line(fname, rows):
    with open(fname, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')
    return True

# python formatter.py [export_billing_csv_filename]
if __name__ == "__main__":
    import sys
    if (len(sys.argv) != 2):
        print 'Usage: python %s [export_billing_csv_filename]' % sys.argv[0]
        quit() 
    fname = sys.argv[1]
    export_billing = load_export_billing_csv_from_file(fname)
    formatted_data = format_rows(export_billing)
    export_json_with_new_line(fname+'.json', formatted_data)
