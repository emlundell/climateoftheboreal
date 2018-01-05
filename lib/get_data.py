#!/bin/env python

# soundings.py

import datetime
import pandas as pd
import numpy as np
import sqlite3
import os


def soundings(query, db):

    # Read sqlite query results into a pandas DataFrame
    home_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
    print("Connecting to {0}/data/soundings/{1}".format(home_path, db))
    con = sqlite3.connect("{0}/data/soundings/{1}".format(home_path, db))
    df = pd.read_sql_query(query, con)

    # verify that result of SQL query is stored in the dataframe
    print(df.size)
    print(df[0:1])

    con.close()

    if 'DATE' in df.columns:
        df.index = df.DATE.apply( lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').date())
        del df["DATE"]

    # For some reason, some values of pressure look to be 10x greater than they should.
    # I'm assuming this is a data report error and correcting accordingly.
    #df[df['PRESS'] > 5000] = df[df['PRESS'] > 5000] / 10.0

    return df
