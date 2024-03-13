import csv
import json
from os.path import join
from os import walk
import argparse
from pathlib import Path

def get_display(file):
    display = {}
    try:
        with open(file) as f:
            reader = csv.reader(f)
            header = None
            for row in reader:
                if header is None:
                    header = row
                #map standard names to display names
                else:
                    display[row[0]] = row[1]
    #could not read file return empty
    except:
        pass
    return display

def get_metadata(file):
    metadata = {}
    try:
        with open(file) as f:
            reader = csv.reader(f)
            header = None
            for row in reader:
                if header is None:
                    header = row
                else:
                    id = row[0]
                    id_metadata = {}
                    metadata[id] = id_metadata
                    for i in range(len(row)):
                        key = header[i]
                        value = row[i]
                        batch_cols = ["t_rh", "sm", "tsoil", "rf"]
                        if key in batch_cols:
                            if value == "nan":
                                value = []
                            else:
                                value = value.split(";")
                        id_metadata[key] = value
    #file does not exist return empty
    except:
        pass
    return metadata


def get_synoptic(file):
    synoptic_data = {}
    try:
        with open(file) as f:
            reader = csv.reader(f)
            header = None
            for row in reader:
                if header is None:
                    header = row
                else:
                    #zip row data
                    row_data = {}
                    for i in range(len(row)):
                        row_data[header[i]] = row[i] if row[i] != "null" else None
                    row_data["unit_conversion_coefficient"] = float(row_data["unit_conversion_coefficient"])
                    if row_data["exclude"] == "":
                        row_data["exclude"] = []
                    else:
                        row_data["exclude"] = row_data["exclude"].split(";")
                    ftype_data = synoptic_data.get(row_data["ftype"])
                    if ftype_data is None:
                        ftype_data = {}
                        synoptic_data[row_data["ftype"]] = ftype_data
                    ftype_data[row_data["standard_name"]] = row_data
    #file does not exist return empty
    except:
        pass
    return synoptic_data


def get_aliases(file):
    alias_data = {}
    try:
        with open(file) as f:
            reader = csv.reader(f)
            header = None
            for row in reader:
                if header is None:
                    header = row
                else:
                    alias_data[row[0]] = row[1]
    #file does not exist return empty
    except:
        pass
    return alias_data


def handle_display_file(in_f, out_f):
    display = get_display(in_f)
    with open(out_f, "w") as f:
        json.dump(display, f, indent = 4)


def handle_metadata_file(in_f, out_f):
    metadata = get_metadata(in_f)
    with open(out_f, "w") as f:
        json.dump(metadata, f, indent = 4)


def handle_syn_file(in_f, out_f):
    synoptic = get_synoptic(in_f)
    with open(out_f, "w") as f:
        json.dump(synoptic, f, indent = 4)


def handle_alias_files(in_dir, out_f):
    aliases = {}
    for root, subdirs, files in walk(in_dir):
            for file in files:
                file = join(root, file)
                path = Path(file)
                ftype = path.parent.name
                id = path.stem
                if aliases.get(ftype) is None:
                    aliases[ftype] = {}
                aliases[ftype][id] = get_aliases(file)

    with open(out_f, "w") as f:
        json.dump(aliases, f, indent = 4)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--out_dir", type = str, help = "The root directory for output json documents.")
    parser.add_argument("-d", "--data_dir", type = str, help = "The root directory where the loggernet station data CSV files to parse are stored.")

    args = parser.parse_args()

    alias_dir = join(args.data_dir, "aliases")
    alias_file_out = join(args.out_dir, "aliases.json")

    syn_file_in = join(args.data_dir, "synoptic/synoptic.csv")
    syn_file_out = join(args.out_dir, "synoptic.json")

    metadata_file_in = join(args.data_dir, "metadata/metadata.csv")
    metadata_file_out = join(args.out_dir, "metadata.json")

    display_file_in = join(args.data_dir, "display/display.csv")
    display_file_out = join(args.out_dir, "display.json")

    handle_metadata_file(metadata_file_in, metadata_file_out)
    handle_syn_file(syn_file_in, syn_file_out)
    handle_alias_files(alias_dir, alias_file_out)
    handle_display_file(display_file_in, display_file_out)

if __name__ == "__main__":
    main()