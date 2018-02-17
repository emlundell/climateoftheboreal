#!/bin/env python3

from __future__ import print_function
import sqlite3
import os
import sys
import argparse
import datetime
import zipfile

"""
    txt_to_db.py './soundings/USM00070261-data-beg2016.txt/USM00070261-data.txt' 'soundings-2016'

    1st: path to text file to be parsed
    2nd: name of sqlite3 DB 

    Results: found in data/DB/ where the DB has two tables - meta and levels.


"""


def nullify(value, check, msg=None):
    if int(value) in check:
        if msg is not None:
            print("{0} has been nullified".format(msg))
        return None
    else:
        return value


def main(path, db_name, time_start='18000101', time_end='21000101'):

    if ".zip" in path:
        with zipfile.ZipFile(path,"r") as zip_ref:
            zip_ref.extractall()
        path = path.strip('.zip')

    # Make sure that the total levels given is the same as said in the metadata
    row_num = 0
    total_level_lines = 0
    level_lines = 0

    # Primary key of meta table to be added to levels
    idpk = None

    # Make new DB
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    # Create tables
    try:
        c.execute('''CREATE TABLE meta
            (   pkid integer PRIMARY KEY,
                HASH text,
                ID text,
                YEAR integer,
                MONTH integer,
                DAY integer,
                HOUR integer,
                RELTIME integer,
                NUMLEV integer,
                P_SRC text,
                NP_SRC text,
                LAT integer,
                LON integer)''')
    except:
        print("Could not create table 'meta'. DOES IT ALREADY EXIST?")

    try:
        c.execute('''CREATE TABLE levels
            (   idpk integer PRIMARY KEY,
                HASH text,
                LVLTYP1 integer,
                LVLTYP2 integer,
                ETIME integer,
                PRESS integer,
                PFLAG text,
                GPH integer,
                ZFLAG text,
                TEMP integer,
                TFLAG text,
                RH integer,
                DPDP integer,
                WDIR integer,
                WSPD integer,
                FOREIGN KEY (idfk) REFERENCES meta(HASH))''')
    except:
        print("Could not create table 'levels'. DOES IT ALREADY EXIST?")

    conn.commit()

    # Get total lines (this may take a while)
    line_count = 0
    with open(path, 'r') as f:
        line_count = sum(1 for _ in f)
    print("total lines: {0}".format(line_count))

    # Open ./soundings/*
    with open(path, 'r') as file:
        for line in file:

            # The negative should always be preceded by a space if followed by a number.
            #print("BEFORE: {0}".format(line))
            #line = re.sub(r'(\-\d)', r' \1', line)
            #print("After: {0}".format(line))

            # Meta
            if line[0] == '#':

                if level_lines != total_level_lines:
                    raise Exception("Bad level line number")

                '''
                # Is index 8 a number? If yes, duplicate 7 to 8
                try:
                    if int(meta[8]):
                        place = meta[7]
                        meta.insert(8, place)
                        print("Warning: copying index 8 to 7 to '{0}'".format(meta))
                except:
                    raise Exception("Bad index at 8 for line '{0}'".format(meta))
                '''

                """
                    ['#USM00070261', '2016', '01', '01', '00', '2303', '183', 'ncdc-nws', 'ncdc-nws', '648161', '-1478767']

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
                """

                try:
                    level = {
                        'HASH': "{0}{1}".format(line[0:23].strip(), nullify(line[24:26], [99], 'meta hash {}'.format(line[0:23].strip()))),
                        'ID': line[0:12],
                        'YEAR': line[13:17],
                        'MONTH': line[18:20],
                        'DAY': line[21:23],
                        'HOUR': nullify(line[24:26], [99]),  # 99
                        'RELTIME': nullify(line[27:31], [9999]),  # 9999
                        'NUMLEV': line[32:36],
                        'P_SRC': line[37:45],
                        'NP_SRC': line[46:54],
                        'LAT': line[55:62],
                        'LON': line[63:71]
                    }
                except:
                    print("Problem with parsing sounding data... Last line parsed:")
                    print("ID YEAR MONTH DAY HOUR RELTIME NUMLEV P_SRC NP_SRC LAT LON")
                    print(line)
                    raise

                if time_start <= datetime.datetime(int(level['YEAR']), int(level['MONTH']), int(level['DAY'])) <= time_end:

                    try:
                        c.execute('''
                            insert or replace into meta values(
                                null,
                                :HASH,
                                coalesce((select ID from meta where hash = :HASH), :ID),
                                coalesce((select YEAR from meta where hash = :HASH), :YEAR),
                                coalesce((select MONTH from meta where hash = :HASH), :MONTH),
                                coalesce((select DAY from meta where hash = :HASH), :DAY),
                                coalesce((select HOUR from meta where hash = :HASH), :HOUR),
                                coalesce((select RELTIME from meta where hash = :HASH), :RELTIME),
                                coalesce((select NUMLEV from meta where hash = :HASH), :NUMLEV),
                                coalesce((select P_SRC from meta where hash = :HASH), :P_SRC),
                                coalesce((select NP_SRC from meta where hash = :HASH), :NP_SRC),
                                coalesce((select LAT from meta where hash = :HASH), :LAT),
                                coalesce((select LON from meta where hash = :HASH), :LON)
                            )
                        ''', level)
                        meta_idpk = c.lastrowid
                    except:
                        print(line)
                        raise

                    total_level_lines = int(level['NUMLEV'])

                    level_lines = 0

                    read = True

                else:
                    read = False

            # Levels
            elif read:

                """
                    30 -9999  -9999   250 -9999 -9999 -9999    90    20

                    LVLTYP1         1-  1   Integer '1'
                    LVLTYP2         2-  2   Integer '1'

                    ETIME           4-  8   Integer '12345'

                    PRESS          10- 15   Integer '123456'
                    PFLAG          16- 16   Character 'a'

                    GPH            17- 21   Integer '12345'
                    ZFLAG          22- 22   Character 'a'

                    TEMP           23- 27   Integer '12345'
                    TFLAG          28- 28   Character 'a'

                    RH             29- 33   Integer '12345'

                    DPDP           35- 39   Integer '12345'

                    WDIR           41- 45   Integer '12345'

                    WSPD           47- 51   Integer '12345'
                """

                level = {

                    'HASH': "{0}{1}".format(meta_idpk, nullify(line[3:8], [-9999, -8888], "levels hash {}".format(meta_idpk))),
                    'LVLTYP1': line[0],
                    'LVLTYP2': line[1],
                    'ETIME': nullify(line[3:8], [-9999, -8888]),  # -9999, -8888
                    'PRESS': nullify(line[9:15], [-9999]),  # -9999
                    'PFLAG': line[15],
                    'GPH': nullify(line[16:21], [-9999, -8888]),  # -9999, -8888
                    'ZFLAG': line[21],
                    'TEMP': nullify(line[22:27], [-9999, -8888]),  # -9999, -8888
                    'TFLAG': line[27],
                    'RH': nullify(line[28:33], [-9999, -8888]),  # -9999, -8888
                    'DPDP': nullify(line[34:39], [-9999, -8888]),  # -9999, -8888
                    'WDIR': nullify(line[40:45], [-9999, -8888]),  # -9999, -8888
                    'WSPD': nullify(line[46:51], [-9999, -8888])  # -9999, -8888
                }

                try:
                    c.execute('''
                        insert or replace into levels values (
                            null,
                            :HASH,
                            coalesce((select LVLTYP1 from levels where hash = :HASH), :LVLTYP1),
                            coalesce((select LVLTYP2 from levels where hash = :HASH), :LVLTYP2),
                            coalesce((select ETIME from levels where hash = :HASH), :ETIME),
                            coalesce((select PRESS from levels where hash = :HASH), :PRESS),
                            coalesce((select PFLAG from levels where hash = :HASH), :PFLAG),
                            coalesce((select GPH from levels where hash = :HASH), :GPH),
                            coalesce((select ZFLAG from levels where hash = :HASH), :ZFLAG),
                            coalesce((select TEMP from levels where hash = :HASH), :TEMP),
                            coalesce((select TFLAG from levels where hash = :HASH), :TFLAG),
                            coalesce((select RH from levels where hash = :HASH), :RH),
                            coalesce((select DPDP from levels where hash = :HASH), :DPDP),
                            coalesce((select WDIR from levels where hash = :HASH), :WDIR),
                            coalesce((select WSPD from levels where hash = :HASH), :WSPD),
                        )''', level)
                except:
                    raise

                level_lines += 1

            row_num += 1
            print("Row number: {0}/{1}  {2}%".format(row_num, line_count, 100.0*row_num/float(line_count)), end="\r")
            sys.stdout.flush()

    conn.commit()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("file", help="Sounding data in text format")
    parser.add_argument("db_name", help="Name of sqlite DB file")
    parser.add_argument("-t", "--time_range", default="18000101_21000101", help="time range to filter soundings (YYYYMMDD_YYYYMMDD)")
    args = parser.parse_args()

    path = args.file  # 'USM00070261-data.txt'
    db_name = args.db_name  # 'soundings-all.db'

    start, end = args.time_range.split("_")

    time_start = datetime.datetime.strptime(start, "%Y%m%d")
    time_end = datetime.datetime.strptime(end, "%Y%m%d")