#!/usr/bin/env python3

import argparse
import hashlib
from time import localtime, strftime, mktime
from datetime import timedelta

import graphql as gq

class Parser():

    def __init__(self, file_name):

        self.file_name = file_name

        print("Get total number of lines in file...")
        with open(self.file_name, "r") as f:
            for total_num_lines, l in enumerate(f, 1):
                pass
        print(f"Total number of lines in file: {total_num_lines}")
        self.total_num_lines = total_num_lines
        self.line_count = 0

        self._start_time()

    def __del__(self):
        self._end_time()

    def _start_time(self):
        self.start_time = localtime()
        print(f"Starting program at {strftime('%a, %d %b %Y %H:%M:%S', self.start_time)}")

    def _end_time(self):
        end_time = localtime()
        print(f"Ending program at {strftime('%a, %d %b %Y %H:%M:%S', end_time)}")
        elasped_secs = mktime(end_time) - mktime(self.start_time)
        print(f"Total elasped time: {timedelta(seconds=elasped_secs)}")

    def line_add_one(self):
        self.line_count += 1
        if self.line_count % 1000 == 0:
            print(f"Processed line {self.line_count} of {self.total_num_lines}")

def nullify(value, nully):
    if not isinstance(nully, list):
        nully = [nully]

    if value in nully:
        return 'null'
    return value

def parse_header(row):
    """
    HEADREC       1-  1  Character
    ID            2- 12  Character
    YEAR         14- 17  Integer
    MONTH        19- 20  Integer
    DAY          22- 23  Integer
    HOUR         25- 26  Integer
    RELTIME      28- 31  Integer
    NUMLEV       33- 36  Integer
    P_SRC        38- 45  Character
    NP_SRC       47- 54  Character
    LAT          56- 62  Integer
    LON          64- 71  Integer

    DB needs unique constraints on ID-YEAR-MONTH-DAY-RELTIME
    So convert ID-YEAR-MONTH-DAY-HOUR to hash and take first 11 digits. This should be unique but deterministic enough.
    """

    if str(row[0]) != '#':
        raise Exception(f"Expected '#' at start of header. Instead got {str(row[0])}")

    header = {
        'id': str(row[1:12]),
        'year': int(row[13:17]),
        'month': int(row[18:20]),
        'day': int(row[21:23]),
        'hour': nullify(int(row[24:26]), 99),
        'reltime': nullify(int(row[27:31]), 9999),
        'numlev': int(row[32:36]),
        'p_src': str(row[37:45]),
        'np_src': str(row[46:54]),
        'lat': int(row[55:62]),
        'lon': int(row[63:71])
    }

    header['header_id'] = hashlib.sha512(f"{header['id']}{header['year']}{header['month']}{header['day']}{header['reltime']}".encode('utf-8')).hexdigest()[0:11]

    # Ingest header into DB
    header_query = """
        mutation upsert_header {
          insert_header(
            objects: [
            {
                header_id: "$header_id",
                hour: $hour,
                lat: $lat,
                day: $day,
                id: "$id",
                lon: $lon,
                month: $month,
                np_src: "$np_src",
                numlev: $numlev,
                p_src: "$p_src",
                reltime: $reltime,
                year: $year
            }
            ],
            on_conflict: {
                constraint: header_pkey,
                update_columns: []
            }
          ) {
            returning {
              header_id,
              numlev
            }
          }
        }
    """

    gq.query(header_query, header)

    return header['header_id'], header['numlev']

def parse_level(header_id, row):
    """
    LVLTYP1         1-  1   Integer
    LVLTYP2         2-  2   Integer
    ETIME           4-  8   Integer
    PRESS          10- 15   Integer
    PFLAG          16- 16   Character
    GPH            17- 21   Integer
    ZFLAG          22- 22   Character
    TEMP           23- 27   Integer
    TFLAG          28- 28   Character
    RH             29- 33   Integer
    DPDP           35- 39   Integer
    WDIR           41- 45   Integer
    WSPD           47- 51   Integer
    """

    level = {
        'header_id': header_id,
        'lvltyp1': int(row[0]),
        'lvltyp2': int(row[1]),
        'etime': nullify(int(row[3:8]), [-9999, -8888]),
        'press': nullify(int(row[9:15]), -9999),
        'pflag': str(row[15]),
        'gph': nullify(int(row[16:21]), [-9999, -8888]),
        'zflag': str(row[21]),
        'temp': nullify(int(row[22:27]), [-9999, -8888]),
        'tflag': str(row[27]),
        'rh': nullify(int(row[28:33]), [-9999, -8888]),
        'dpdp': nullify(int(row[34:39]), [-9999, -8888]),
        'wdir': nullify(int(row[40:45]), [-9999, -8888]),
        'wspd': nullify(int(row[46:51]), [-9999, -8888])
    }

    # Ingest levels into DB
    level_query = """
        mutation upsert_levels {
          insert_levels(
            objects: [
            {
                dpdp: $dpdp,
                etime: $etime,
                gph: $gph,
                header_id: "$header_id",
                lvltyp1: $lvltyp1,
                lvltyp2: $lvltyp2,
                pflag: "$pflag",
                press: $press,
                rh: $rh,
                temp: $temp,
                tflag: "$tflag",
                wdir: $wdir,
                wspd: $wspd,
                zflag: "$zflag"
            }],
            on_conflict: {
                constraint: levels_pkey,
                update_columns: []
            }
          ) {
            returning {
                header_id,
                level_id
            }
          }
        }
    """

    gq.query(level_query, level)

def parse_and_ingest(file_name):

    # Open radiosonde file
    # It is assumed that the file always starts with a header
    parse = Parser(file_name)

    print("Parse and ingest file...")
    with open(file_name, "r") as f:
        while True:
            header_id, num_lev = parse_header(f.read(72))  # Includes newline at end
            parse.line_add_one()
            for r in range(0, num_lev):
                parse_level(header_id, f.read(53))  # Includes newline at end
                parse.line_add_one()

def setup_db():
    """
        CREATE TABLE header (
            header_id TEXT PRIMARY KEY,
            id TEXT,
            year INT,
            month INT,
            day INT,
            hour INT,
            reltime INT,
            numlev INT,
            p_src TEXT,
            np_src TEXT,
            lat INT,
            lon INT
        );

        CREATE TABLE
        levels (
            level_id SERIAL PRIMARY KEY,
            header_id TEXT REFERENCES header(header_id),
            lvltyp1 INT,
            lvltyp2 INT,
            etime INT,
            press INT,
            pflag TEXT,
            gph INT,
            zflag TEXT,
            temp INT,
            tflag TEXT,
            rh INT,
            dpdp INT,
            wdir INT,
            wspd INT
        );
    """

    # !!! AFTER CREATING TABLES, GO TO HASURA'S UI AND ADD TABLES TO SCHEMA

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest radiosonde data into DB')
    parser.add_argument('-f', '--file_name', help='File of radiosonde data')
    args = parser.parse_args()

    if args.file_name:
        parse_and_ingest(args.file_name)
