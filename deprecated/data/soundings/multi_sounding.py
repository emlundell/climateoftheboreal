#! /bin/usr/env python3

import multiprocessing as mp
import datetime
import argparse
import zipfile
import sqlite3


def nullify(value, check):
    try:
        if int(value) in check:
            return 'null'
        else:
            return value
    except:  # Assume str
        if str(value) in check:
            return 'null'
        else:
            return value


def sounding(data, file_meta):

    db_name, time_start, time_end, time0 = file_meta

    meta_statements = ""
    levels_statements = ""

    # header
    line = data[0:71]

    try:
        meta = {
            'HASH': "{0}{1}".format(line[0:26].replace(' ', '').replace('#', ''), line[27:31]),
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

    # Some sanity checks
    try:
        _ = int(meta['YEAR'])
    except:
        print("Problem with year '{0}' on line {1}".format(meta['YEAR'], line))
        raise

    #print("line/meta: ", line, meta)
    if time_start <= datetime.datetime(int(meta['YEAR']), int(meta['MONTH']), int(meta['DAY'])) <= time_end:

        try:
            meta_statements += """
    ('{HASH}', '{ID}', {YEAR}, {MONTH}, {DAY}, {HOUR}, {RELTIME}, {NUMLEV},
    '{P_SRC}', '{NP_SRC}', {LAT}, {LON}),""".format(**meta)

        except:
            print(line)
            raise

        last = 71
        for i in range(int(meta['NUMLEV'])):

            line = data[last+i:last+i+51]
            level = {
                'HASH': meta['HASH'],
                'LVLTYP1': line[0],
                'LVLTYP2': line[1],
                'ETIME': nullify(line[3:8], [-9999, -8888]),  # -9999, -8888
                'PRESS': nullify(line[9:15], [-9999]),  # -9999
                'PFLAG': line[15],
                'GPH': nullify(line[16:21], [-9999, -8888]),  # -9999, -8888
                'ZFLAG': line[21],
                'TEMP': nullify(line[22:27], [-9999, -8888]),  # -9999, -8888
                'TFLAG': nullify(line[27], [' ']),
                'RH': nullify(line[28:33], [-9999, -8888]),  # -9999, -8888
                'DPDP': nullify(line[34:39], [-9999, -8888]),  # -9999, -8888
                'WDIR': nullify(line[40:45], [-9999, -8888]),  # -9999, -8888
                'WSPD': nullify(line[46:51], [-9999, -8888])  # -9999, -8888
            }

            #print("line/level: ", line, level)

            try:
                levels_statements += """
('{HASH}', {LVLTYP1}, {LVLTYP2}, {ETIME}, {PRESS}, '{PFLAG}', {GPH}, '{ZFLAG}', {TEMP}, {TFLAG}, {RH}, {DPDP}, {WDIR}, {WSPD}),""".format(**level)
            except:
                raise

            last += 51

        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        stm = """
    insert into meta
    ('HASH', 'ID', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'RELTIME', 'NUMLEV', 'P_SRC', 'NP_SRC', 'LAT', 'LON') VALUES
    {0}""".format(meta_statements[0:-1])
        #print("Execute many meta...{0}...at {1}".format(stm, datetime.datetime.now()))
        c.execute(stm)

        stm = """
    insert into levels
    ('HASH', 'LVLTYP1', 'LVLTYP2', 'ETIME', 'PRESS', 'PFLAG', 'GPH', 'ZFLAG', 'TEMP', 'TFLAG', 'RH', 'DPDP', 'WDIR', 'WSPD') VALUES
    {0}""".format(levels_statements[0:-1])
        #print("Execute many levels....{0}".format(stm, datetime.datetime.now()))
        c.execute(stm)

        conn.commit()

        #print("Done...{0}".format(datetime.datetime.now()))
        conn.close()

        return datetime.datetime.now() - time0, time0

done = 0
def job_done(times):
    global done
    done += 1
    if not done % 1000:
        print("Number of jobs done {0} since {1}: {2}".format(times[0], times[1], done))


def main(file_name='USM00070261-data.txt', db_name='multisounding.db', time_start='18000101', time_end='21000101'):

    time0 = datetime.datetime.now()
    print("Opening {0} at {1}".format(file_name, time0))

    if isinstance(time_start, str):
        time_start = datetime.datetime.strptime(time_start, "%Y%m%d")

    if isinstance(time_end, str):
        time_end = datetime.datetime.strptime(time_end, "%Y%m%d")

    if ".zip" in file_name:
        with zipfile.ZipFile(file_name,"r") as zip_ref:
            zip_ref.extractall()
        file_name = file_name.strip('.zip')

    with open(file_name, 'r') as f:
        print("Reading data into huge string...")
        data = f.read().replace('\n', '')

    print("Finding '#'...")
    # Get number of profiles to parse
    pos_1 = (pos for pos, char in enumerate(data) if char == '#')
    pos_2 = (pos for pos, char in enumerate(data) if char == '#')

    # init objects
    pool = mp.Pool(mp.cpu_count() or 1)
    #pool = mp.Pool(1)
    jobs = []

    print("Make or update meta table")
    # Make new DB
    conn = sqlite3.connect(db_name)
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    # Create tables
    c.execute('''CREATE TABLE meta
        (
            HASH text UNIQUE,
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
            LON integer
        )''')

    c.execute('''CREATE TABLE levels
        (
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
            FOREIGN KEY (HASH) REFERENCES meta(HASH)
        )''')

    conn.commit()
    c.close()

    print("Looping through...")
    _ = next(pos_2)
    for i, p1 in enumerate(pos_1):
        if not i % 1000:
            print("Step {0}, {1} seconds after starting.".format(i, datetime.datetime.now() - time0))
        try:
            p2 = next(pos_2)
        except:
            p2 = -1
        jobs.append(pool.apply_async(sounding, (data[p1:p2], (db_name, time_start, time_end, time0)), callback=job_done))

    # clean up
    print("Close and Join. Time since started: {0}".format(datetime.datetime.now() - time0))
    pool.close()
    pool.join()

    print("End after time {0}, started at {1}".format(datetime.datetime.now() - time0, time0))

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

    main(path, db_name, time_start, time_end)
