
#import matplotlib as mp
import matplotlib.pyplot as plt

import datetime
import pandas as pd
import numpy as np
import sqlite3

import default
from get_data import soundings

query = '''
	select
		m.year || '-' || m.month || '-' || m.day as DATE,
		cast(strftime('%j', 2016 || '-' || substr('0' || m.month, -2) || '-' || substr('0' || m.day, -2) ) as int) as DOY,
		v.gph, 
		v.press/10.0 as PRESS, 
		v.TEMP/10.0 as TEMP,
		v.lvltyp1 
	from levels v 
		join meta m on v.idfk = m.idpk 
	where v.lvltyp1 <> 3 and
		m.hour = 0
	'''

db = "soundings-2016.db"

df = soundings(query, db)

# Graph
#x_arr = df['GPH']
x_arr = df['TEMP']
y_arr = df['PRESS']
colors = list(df['DOY'])

plt.scatter(x_arr, y_arr, c=colors, cmap=plt.cm.BrBG)
plt.yscale('log')
plt.axis([min(x_arr), max(x_arr), max(y_arr), min(y_arr)])
plt.title("Fairbanks, AK Soundings for 2016")
plt.xlabel("Temp (C)")
plt.ylabel("Pressure (mb)")
plt.savefig("soundings_2016.png")
plt.close()


# Let's plot the temp over the year for standard heights
stdf = df[df['LVLTYP1'] == 1]

# Do not plot level 10000 and 7
stdf = stdf[stdf['PRESS'] != 7]
stdf = stdf[stdf['PRESS'] != 10000]

unique_levels = list((stdf['PRESS']).unique())
levels = {index: key for key, index in enumerate(unique_levels)}
colors = [levels[i] for i in list(stdf['PRESS'])]

"""
plt.scatter(stdf['DOY'], stdf['TEMP'], marker='o', c=colors, cmap=plt.cm.jet)
plt.title("Fairbanks, AK Sounding Temp over 2016")
plt.xlabel("Day of year")
plt.ylabel("Temp (C)")
plt.savefig("soundings_temp_over_year_scatter.png")
plt.close()
"""

grouped = stdf.groupby('PRESS')

color=iter(plt.cm.rainbow(np.linspace(0,1,len(unique_levels))))
for i, (k,g) in enumerate(grouped):
	c = next(color)
	plt.plot(g["TEMP"].index, g["TEMP"], c=c, label=k)
plt.title("Fairbanks, AK Sounding Temp over 2016")
plt.ylabel("Temp (C)")
plt.legend(title="Pressure (mb)", loc='center left', bbox_to_anchor=(1, 0.5))
plt.gcf().autofmt_xdate()
plt.savefig("soundings_temp_over_year_line.png", bbox_inches='tight')
plt.close()