#!/usr/bin/env python3

import string

import requests


# https://stackoverflow.com/a/50514619
def make_query(query, variables=None):

    if variables is None:
        variables = {}

    url = "http://localhost:8080/v1/graphql"

    json_query = string.Template(query).substitute(variables)

    request = requests.post(url, json={'query': json_query})
    if request.status_code == 200:
        ret = request.json()
        try:
            return ret['data']
        except Exception:
            raise Exception(f"Query failed to run: {ret}")
    else:
        raise Exception(f"Query failed to run by returning code of {request.status_code}. {json_query}")
