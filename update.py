import csv

# variable_metadata_new = []
# unit_metadata = []
# found_units = set()

header = None

# with open("csv_data/variables/variable_metadata.csv", "r") as f:
#   reader = csv.reader(f)
#   header = next(reader)
#   for row in reader:
#     new_row = row[:3]
#     variable_metadata_new.append(new_row)
#     unit_data = row[2:]
#     unit = unit_data[0]
#     if unit and unit not in found_units:
#       unit_metadata.append(unit_data)
#       found_units.add(unit)
      
    

# with open("csv_data/variables/variable_metadata_2.csv", "w") as f:
#   writer = csv.writer(f)
#   writer.writerow(header[:3])
#   writer.writerows(variable_metadata_new)

# with open("csv_data/variables/units.csv", "w") as f:
#   writer = csv.writer(f)
#   writer.writerow(header[2:])
#   writer.writerows(unit_metadata)

with open("csv_data/variables/version_translations.csv", "r") as f:
  reader = csv.reader(f)
  header = next(reader)
  header.append("unit_conversion")
  for row in reader:
    if "ClimaVUE50 (535) Recipe" in row[-1] and row[1] in ["VP", "BP"]:
      row.append("hpa_kpa")
      print(row)
    else:
      row.append("")