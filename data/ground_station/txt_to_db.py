#!/bin/env python3

from __future__ import print_function
import sqlite3
import os
import sys
import argparse
import datetime

"""
    Parse ISD into DB

    txt_to_db.py './ground_data/702610-26411-2018.lite' 'ground.db'

    1st: path to text file to be parsed
    2nd: name of sqlite3 DB 

    Results: found in DB in table 'ground_data'.


"""

parser = argparse.ArgumentParser()

parser.add_argument("file", help="Integrated Surface data in text format")
parser.add_argument("db_name", help="Name of sqlite DB file to be saved")
parser.add_argument("-t", "--time_range", default="18000101_21000101", help="time range to filter soundings (YYYYMMDD_YYYYMMDD)")
args = parser.parse_args()

path = args.file  # '702610-26411-2018.lite'
db_name = args.db_name  # 'ground.db'

start, end = args.time_range.split("_")

time_start = datetime.datetime.strptime(start, "%Y%m%d")
time_end = datetime.datetime.strptime(end, "%Y%m%d")


def nullify(value, check):
    try:
        _ = int(value)
    except:
        print("WARNING: Nullify value of '{0}' can't be cast as an int.".format(value))

    if int(value) in check:
        return None
    else:
        return value


# Make sure that the total levels given is the same as said in the metadata
total_level_lines = 0

# Primary key of meta table to be added to levels
idpk = None

# Make new DB
conn = sqlite3.connect(db_name)
c = conn.cursor()

try:
    # Create tables
    c.execute('''CREATE TABLE ground
        (   DATE text PRIMARY KEY, 
            ID text, --702610
            YEAR integer, 
            MONTH integer, 
            DAY integer,
            HOUR integer, 
            TEMP integer, 
            DEW integer, 
            MSL integer, 
            WIND_DIR integer, 
            WIND_SPEED integer,
            CLOUD text, 
            LIQUID_ONE integer, 
            LIQUID_SIX integer)''')
except:
    print("Could not create table. DOES IT ALREADY EXIST?")

conn.commit()

# Get total lines (this may take a while)
line_count = 0
with open(path, 'r') as f:
    line_count = sum(1 for _ in f)
print("total lines: {0}".format(line_count))

with open(path, 'r') as file:
    for row_num, line in enumerate(file):

        '''
        ID text, --702610
        DATE text, 
        YEAR integer, 
        MONTH integer, 
        DAY integer,
        HOUR integer, 
        TEMP integer, -- * 10 C, missing -9999
        DEW integer,  -- * 10 C, missing -9999
        MSL integer, -- * 10 hP, missing -9999
        WIND_DIR integer, -- angular, missing -9999
        WIND_SPEED integer, -- m/s, missing -9999
        CLOUD text, -- missing -9999
        LIQUID_ONE integer, -- trace -1, mm, missing -9999 
        LIQUID
        '''

        if "{0}-{1}-{2}".format(line[0:4].strip(), line[5:7].strip(), line[8:11].strip()) == '2017-02-24':
            print(line)

        try:
            ground = {
                'DATE': "{0}-{1}-{2}-{3}".format(line[0:4].strip(), line[5:7].strip(), line[8:10].strip(), line[11:13].strip()),
                'ID': 702610,
                'YEAR': line[0:4],
                'MONTH': line[5:7],
                'DAY': line[8:10],
                'HOUR': line[11:13],
                'TEMP': nullify(line[14:19], [-9999]),
                'DEW': nullify(line[20:25], [-9999]),
                'MSL': nullify(line[26:31], [-9999]),
                'WIND_DIR': nullify(line[32:37], [-9999]),
                'WIND_SPEED': nullify(line[38:43], [-9999]),
                'CLOUD': nullify(line[44:49], [-9999]),
                'LIQUID_ONE': nullify(line[50:55], [-9999]),
                'LIQUID_SIX': nullify(line[56:61], [-9999])
            }

        except:
            print("Problem with parsing ground data... Last line parsed:")
            print("YEAR MONTH DAY HOUR TEMP DEW MSL WIND_DIR WIND_SPEED CLOUD LIQUID_ONE LIQUID_SIX")
            print(line)
            raise

        if time_start <= datetime.datetime(int(ground['YEAR']), int(ground['MONTH']), int(ground['DAY'])) <= time_end:

            try:
                c.execute('''
                    insert or replace into ground values (
                        :DATE,
                        coalesce((select ID from ground where date = :DATE), :ID),
                        coalesce((select YEAR from ground where date = :DATE), :YEAR),
                        coalesce((select MONTH from ground where date = :DATE), :MONTH),
                        coalesce((select DAY from ground where date = :DATE), :DAY),
                        coalesce((select HOUR from ground where date = :DATE), :HOUR),
                        coalesce((select TEMP from ground where date = :DATE), :TEMP),
                        coalesce((select DEW from ground where date = :DATE), :DEW),
                        coalesce((select MSL from ground where date = :DATE), :MSL),
                        coalesce((select WIND_DIR from ground where date = :DATE), :WIND_DIR),
                        coalesce((select WIND_SPEED from ground where date = :DATE), :WIND_SPEED),
                        coalesce((select CLOUD from ground where date = :DATE), :CLOUD),
                        coalesce((select LIQUID_ONE from ground where date = :DATE), :LIQUID_ONE),
                        coalesce((select LIQUID_SIX from ground where date = :DATE), :LIQUID_SIX)
                    )
                ''', ground)
            except:
                print("Could not insert or replace into DB.")
                raise

        print('')
        print("Row number: {0}/{1}  {2}%".format(row_num, line_count, 100.0*row_num/float(line_count)), end="\r")
        sys.stdout.flush()

conn.commit()
conn.close()
