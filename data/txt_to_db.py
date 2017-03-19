#!/bin/env python3

from __future__ import print_function
import sqlite3
import os
import re
import sys

"""
	txt_to_db.py './soundings/USM00070261-data-beg2016.txt/USM00070261-data.txt' 'soundings-2016'

	1st: path to text file to be parsed
	2nd: name of sqlite3 DB 

	Results: found in data/DB/ where the DB has two tables - meta and levels.


"""	 
# Might add in command line options later.  But let's manully change it for now.
#path = './soundings/USM00070261-data-beg2016.txt'
#db_name = 'soundings-2016' + '.db'
path = './soundings/USM00070261-data.txt'
db_name = 'soundings-all.db'

def nullify(value, check):
	if int(value) in check:
		return None
	else:
		return value

# Make sure that the total levels given is the same as said in the metadata
row_num = 0
total_level_lines = 0
level_lines = 0

# Primary key of meta table to be added to levels
idpk = None

# Remove old DB
try:
	os.remove(db_name)
except:
	pass

# Make new DB
conn = sqlite3.connect(db_name)
conn.execute("PRAGMA foreign_keys = 1")
c = conn.cursor()

# Create tables
c.execute('''CREATE TABLE meta
	(idpk integer PRIMARY KEY, 
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

c.execute('''CREATE TABLE levels
	(idpk integer PRIMARY KEY, 
		idfk integer,  
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
		FOREIGN KEY (idfk) REFERENCES meta(idpk))''')

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

			level = {

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

			try:
				
				c.execute('''
					insert into meta values 
					(Null,:ID,:YEAR,:MONTH,:DAY,:HOUR,:RELTIME,
						:NUMLEV,:P_SRC,:NP_SRC,:LAT,:LON)''', level)
				idpk = c.lastrowid
			except:
				print(line)
				raise
				
			#print("Meta rowid = {0}".format(idpk))
			#conn.commit()

			total_level_lines = int(level['NUMLEV'])

			level_lines = 0
			
		# Levels
		else:

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
				
				'idfk': idpk,
				'LVLTYP1': line[0],
				'LVLTYP2': line[1],
				'ETIME': nullify(line[3:8], [-9999, -8888]), # -9999, -8888
				'PRESS': nullify(line[9:15], [-9999]), # -9999
				'PFLAG': line[15],
				'GPH': nullify(line[16:21], [-9999, -8888]), # -9999, -8888
				'ZFLAG': line[21],
				'TEMP': nullify(line[22:27], [-9999, -8888]), # -9999, -8888
				'TFLAG': line[27],
				'RH': nullify(line[28:33], [-9999, -8888]), # -9999, -8888
				'DPDP': nullify(line[34:39], [-9999, -8888]), # -9999, -8888
				'WDIR': nullify(line[40:45], [-9999, -8888]), # -9999, -8888
				'WSPD': nullify(line[46:51], [-9999, -8888]) # -9999, -8888
			}


			'''
			print(line)
			for i in ['idfk','LVLTYP1','LVLTYP2','ETIME','PRESS','PFLAG','GPH','ZFLAG','TEMP','TFLAG','RH','DPDP','WDIR','WSPD']:
				print(i, level[i])
			'''

			try:
				c.execute('''
					insert into levels values(Null,:idfk,:LVLTYP1,:LVLTYP2,
						:ETIME,:PRESS,:PFLAG,:GPH,:ZFLAG,:TEMP,:TFLAG,:RH, 
						:DPDP,:WDIR,:WSPD)''', level)
			except:
				raise
			
			level_lines += 1

		row_num += 1
		print("Row number: {0}/{1}  {2}%".format(row_num, line_count, 100.0*row_num/float(line_count)), end="\r")
		sys.stdout.flush()

conn.commit()
conn.close()