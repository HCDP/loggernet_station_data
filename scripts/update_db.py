from os import environ
import csv
import psycopg2
import pathlib

def load_csv(file, table, cur):
    with open(file, newline = '') as f:
        reader = csv.reader(f)
        row_len = len(next(reader))
        load_rows(table, row_len, reader, cur)
        
        
def load_versions(file, table, cur):
    rows = []
    #process file name
    with open(file, newline = '') as f:
        reader = csv.reader(f)
        row_len = len(next(reader))
        for row in reader:
            row[2] = int(row[2]) if row[2] != "NA" else None
            programs = row[3]
            program_list = programs.split(";")
            rows += [[*row[:3], f"CPU:{program}"] for program in program_list]
        
        #TEMPORARY
        keys = set()
        filtered = []
        for row in rows:
            key = f"{row[1]}_{row[3]}"
            if key not in keys:
                keys.add(key)
                filtered.append(row)
        
        load_rows(table, row_len, filtered, cur)
            
            
def load_rows(table, row_len, rows, cur):
    params = ["%s" for i in range(row_len)]
    param_str = ",".join(params)
    param_str = f"({param_str})"
    #mogrify will handle query sanitation
    #replace empty strings with null
    values = ",".join(cur.mogrify(param_str, [None if value == '' else value for value in row]).decode('utf-8') for row in rows)
    if len(values) > 0:
        cur.execute(f"INSERT INTO {table} VALUES {values}")
            
            
table_name_whitelist = ["sensor_positions", "station_metadata", "synoptic_exclude", "synoptic_translations", "variable_metadata", "version_translations"]

with psycopg2.connect(
    host = environ["DB_HOST"], 
    port = environ.get("DB_PORT") or "5432", 
    dbname = environ["DB_NAME"], 
    user = environ["DB_USERNAME"], 
    password = environ["DB_PASSWORD"]
) as conn:
    # Open a cursor to perform database operations
    with conn.cursor() as cur:
        change_file = "changes.txt"
        with open(change_file) as cf:
            for file in cf:
                file = file.strip()
                fdata = pathlib.Path(file)
                table_name = fdata.stem
                #validate table name and ensure this is a csv file
                if fdata.suffix == ".csv" and table_name in table_name_whitelist:
                    # Execute a test query
                    cur.execute(f"TRUNCATE TABLE {table_name};")
                    conn.commit()
                    if table_name == "version_translations":
                        load_versions(file, table_name, cur)
                    else:
                        load_csv(file, table_name, cur)
