#! /bin/usr/env python3

import yaml

with open("data.yaml") as stream:
    try:
        print(yaml.load(stream))
    except yaml.YAMLError as exc:
        print(exc)


