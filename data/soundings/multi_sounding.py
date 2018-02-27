
import multiprocessing as mp
import datetime


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


def sounding(file_name, pos):

    with open(file_name, 'r') as f:

        f.seek(pos)

        # header
        line = f.readline()

        try:
            level = {
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

        if time_start <= datetime.datetime(int(level['YEAR']), int(level['MONTH']), int(level['DAY'])) <= time_end:

            try:
                meta_statements += """
        ('{HASH}', '{ID}', {YEAR}, {MONTH}, {DAY}, {HOUR}, {RELTIME}, {NUMLEV},
        '{P_SRC}', '{NP_SRC}', {LAT}, {LON}),""".format(**level)

            except:
                print(line)
                raise

            for i in int(level['NUMLEV']):
                level = {
                    'HASH': level['HASH'],
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

                try:
                    levels_statements += """
('{HASH}', {LVLTYP1}, {LVLTYP2}, {ETIME}, {PRESS}, '{PFLAG}', {GPH}, '{ZFLAG}', {TEMP}, {TFLAG}, {RH}, {DPDP}, {WDIR}, {WSPD}),""".format(**level)
                except:
                    raise

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

            print("Execute many meta...{0}".format(datetime.datetime.now()))
            stm = """
        insert into meta
        ('HASH', 'ID', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'RELTIME', 'NUMLEV', 'P_SRC', 'NP_SRC', 'LAT', 'LON') VALUES
        {0}""".format(meta_statements[0:-1])
            c.execute(stm)
            conn.commit()

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

            print("Execute many levels....{0}".format(datetime.datetime.now()))
            stm = """
        insert into levels
        ('HASH', 'LVLTYP1', 'LVLTYP2', 'ETIME', 'PRESS', 'PFLAG', 'GPH', 'ZFLAG', 'TEMP', 'TFLAG', 'RH', 'DPDP', 'WDIR', 'WSPD') VALUES
        {0}""".format(levels_statements[0:-1])
            c.execute(stm)
            conn.commit()

            print("Done...{0}".format(datetime.datetime.now()))
            conn.close()


def main():

    file_name = 'USM00070261-data.txt'

    print("Opening {0}".format(file_name))

    with open(file_name, 'r') as f:

        print("Reading data into huge string...")
        data = f.read().replace('\n', '')

        print("Finding '#'...")
        time0 = datetime.datetime.now()
        a = (pos for pos, char in enumerate(data) if char == '#')

        # init objects
        #pool = mp.Pool(mp.cpu_count() or 1)
        pool = mp.Pool(1)
        jobs = []

        print("Looping through...")
        for p in a:
            raw_input("Hit enter to continue...")
            jobs.append(pool.apply_async(sounding, (file_name, p)))

        # wait for all jobs to finish
        for job in jobs:
            job.get()

        # clean up
        pool.close()

        print("End at time {0}".format(datetime.datetime.now() - time0))
