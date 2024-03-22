from urllib.request import urlopen
import csv
from io import StringIO

master_meta_file = "https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv"
name_map = {}
with urlopen(master_meta_file) as f:
    decoded = f.read().decode()
    text = StringIO(decoded)
    reader = csv.reader(text)
    line_number = 0
    for line in reader:
        station_id = line[0]
        station_name = line[1]
        name_map[station_id] = station_name

meso_meta_file = "csv_data/metadata/metadata.csv"

updated_data = []
with open(meso_meta_file) as f:
    reader = csv.reader(f)
    for row in reader:
        if len(updated_data) == 0:
            row.insert(1, "name")
        else:
            station_id = row[0]
            station_name = name_map[station_id]
            row.insert(1, station_name)
        updated_data.append(row)
print(updated_data)
