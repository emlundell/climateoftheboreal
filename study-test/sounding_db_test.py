
#import matplotlib as mp
import matplotlib.pyplot as plt
#matplotlib.use('Agg')

import datetime
import pandas as pd
import numpy as np
import sqlite3
import default

from get_data import soundings

# Read sqlite query results into a pandas DataFrame
db = "soundings-2016.db"
query = '''
	select
		cast(strftime('%j', 2016 || '-' || substr('0' || m.month, -2) || '-' || substr('0' || m.day, -2) ) as int) as DOY, 
		v.gph, 
		v.press/10.0 as PRESS, 
		v.TEMP/10.0 as TEMP 
	from levels v 
		join meta m on v.idfk = m.idpk 
	where v.lvltyp1 <> 3
		--and m.month = 12 and
		--m.day = 20
	'''
df = soundings(query, db)

# Graph
#x_arr = df['GPH']
x_arr = df['TEMP']
y_arr = df['PRESS']
colors = list(df['DOY'])

plt.scatter(x_arr, y_arr, c=colors, cmap=plt.cm.BrBG)
plt.yscale('log')
plt.axis([min(x_arr), max(x_arr), max(y_arr), min(y_arr)])
plt.savefig("sounding_test.png")
