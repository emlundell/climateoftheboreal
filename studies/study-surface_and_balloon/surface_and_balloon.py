#! /usr/bin python3

from random import randint

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from get_data import soundings
from get_data import surface
from extras import since_bc

"""
# Sounding temp
# Read sqlite query results into a pandas DataFrame
db = "soundings-70261.db"
query = '''
    select
        substr('0000' || m.year, -4) || '-' || substr('0' || m.month, -2) || '-' || substr('0' || m.day, -2) as DATE, 
        v.gph, 
        v.press/10.0 as PRESS, 
        v.TEMP/10.0 as TEMP 
    from levels v 
        join meta m on v.idfk = m.idpk 
    where v.lvltyp2 = 1  -- surface
        --and m.month = 12
        --and m.day = 25
        and m.hour = 0
        and m.year >= 1946 and m.year <= 2017
    '''
df = soundings(query, db)

# Graph
x_arr_s = df['TEMP']
y_arr_s = since_bc(df['DATE'])

plt.plot_date(y_arr_s, x_arr_s)
plot_name = "sounding_test_{0}.png".format(randint(0, 10000))
#plt.savefig(plot_name)
print("Saved to {0}".format(plot_name))


# Ground temp
db = "ground-70261.db"
query = '''
    select
        g.DATE as DATE, 
        g.TEMP/10.0 as TEMP
    from ground g
    where g.hour = 0
        and g.year >= 1946 and g.year <= 2017
'''

df = surface(query, db)

# Graph
x_arr_g = df['TEMP']
y_arr_g = since_bc(df['DATE'])

plt.plot_date(y_arr_g, x_arr_g)
plot_name = "ground_test_{0}.png".format(randint(0, 10000))
#plt.savefig(plot_name)
print("Saved to {0}".format(plot_name))
"""

db = "soundings-70261.db"
query = '''
    ATTACH DATABASE 'ground-70261.db' AS GROUND;
    
    select
        v.TEMP/10.0 as STEMP,
        g.TEMP/10.0 as GTEMP 
    from levels v 
        join meta m on v.idfk = m.idpk
        join GROUND.ground g on g.year = m.year and g.month = m.month and g.day = m.day and g.hour = m.hour
    where v.lvltyp2 = 1  -- surface
        and m.hour = 0
        and m.year >= 1946 and m.year <= 2017
    '''
df = soundings(query, db)


plt.scatter(df['STEMP'], df['GTEMP'])
plot_name = "ground_sounding_test_scatter_{0}.png".format(randint(0, 10000))
plt.savefig(plot_name)
print("Saved to {0}".format(plot_name))
