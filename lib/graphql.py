#!/usr/bin/env python3

import string
import json

import requests
from pandas.io.json import json_normalize as jn

def query(query, v=None, top=None, return_json=False):
    """
    Given a qraphql query, return the results as a normalized Pandas dataframe.

    top: Top level object in query that will be returned. Makes parsing datatables easier.

    v: A dict containing the values of the placeholders
        $myvar => {'myvar': 123}

    return_json: If true, function returns a json string instead.
    """

    if v is None:
        v = {}

    url = "http://localhost:8080/v1/graphql"

    json_query = string.Template(query).substitute(v)
    request = requests.post(url, json={'query': json_query})
    if request.status_code == 200:
        ret = request.json()
        try:
            if return_json:
                return json.dumps(ret['data'])

            if top is None:
                return jn(ret['data'])

            return jn(ret['data'][top])

        except Exception as e:
            raise Exception(f"Query failed to run: {ret}. {e}")
    else:
        raise Exception(f"Query failed to run by returning code of {request.status_code}. {json_query}")
