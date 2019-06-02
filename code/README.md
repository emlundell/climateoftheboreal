
_data_ is where the raw climate data goes before it is processed.

_db_ folder is where the db data goes. This data is often times a Postgres DB file.

_graphql.yaml_ is a docker compose file for starting up Hasura and Postgres.

_radiosonde.py_ is a Python script to ingest raw radiosonde data into Postgres.


# Setup graphql
## Prereqs

Make sure that docker engine is installed and running

## Install Hasura
https://docs.hasura.io/1.0/graphql/manual/getting-started/docker-simple.html

1. wget https://raw.githubusercontent.com/hasura/graphql-engine/master/install-manifests/docker-compose/docker-compose.yaml
1. Change value of _service.postgres.volumes.db_data_ to proper _data_ directory.
1. docker-compose up -d
1. docker ps
1. http://localhost:8080/console


# Ingest Radiosonde data
## Download radiosonde data

1. Go to https://www.ncdc.noaa.gov/data-access/weather-balloon/integrated-global-radiosonde-archive to get familiar with the available products

1. Look at http://www1.ncdc.noaa.gov/pub/data/igra/igra2-station-list.txt to find the station id wanted.

  `USM00070261  64.8161 -147.8767  133.7 AK FAIRBANKS/INT.                 1930 2019  66329`
1. Find the station id at https://www1.ncdc.noaa.gov/pub/data/igra/.
  - Use *data/data_por/* for all data.
  - Use *data/data_y2d/* for all data since beginning of the current year.


1. Put zip file in _data_ directory and unzip


## Run radiosonde.py

1. Install pipenv (if needed)
  `pip3 install --user pipenv`
  `export PATH="${HOME}/.local/bin:$PATH"`

1. Initialize pipenv environment (if needed)
  `pip3 --version`
  `sudo pipenv --python 3.6`

1. Install dependencies
  `sudo pipenv install requests`

1. To uninstall pipenv environments
  `sudo pipenv --rm`

1. Start pipenv shell
  `sudo pipenv shell`
