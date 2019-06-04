
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
1. Change value of _service.postgres.volumes to _${pwd}/data/db_sym/:/var/lib/postgresql/_
1. _docker-compose up -d_
1. _docker ps_
1. http://localhost:8080/console
1. To shutdown servers: _docker-compose down_


# Ingest Radiosonde data
## Download radiosonde data

1. Go to https://www.ncdc.noaa.gov/data-access/weather-balloon/integrated-global-radiosonde-archive to get familiar with the available products

1. Look at http://www1.ncdc.noaa.gov/pub/data/igra/igra2-station-list.txt to find the station id wanted.

  `USM00070261  64.8161 -147.8767  133.7 AK FAIRBANKS/INT.                 1930 2019  66329`
1. Find the station id at https://www1.ncdc.noaa.gov/pub/data/igra/.
  - Use *data/data_por/* for all data.
  - Use *data/data_y2d/* for all data since beginning of the current year.


1. Put zip file in _data_ directory and unzip


## Setup python env

1. Install pipenv (if needed)

  ```bash
  pip3 install --user pipenv
  export PATH="${HOME}/.local/bin:$PATH"
  ```

1. Make sure you are that repo's root. Pipenv uses the cwd for the virtual env naming.

1. Initialize pipenv environment (if needed)

  ```bash
  pip3 --version
  sudo pipenv --python 3.6
  ```

1. Install dependencies
  `sudo pipenv install requests jupyter pandas cufflinks plotly`

1. To uninstall pipenv environments:

  `sudo pipenv --rm`

1. Start pipenv shell

  `pipenv shell`

1. Set some python paths. Need to do this whenever a new shell is created

  `export PYTHONPATH=$PYTHONPATH:$HOME/GIT/climateoftheboreal/lib/`

1. Make sure that postgres and Hasura are running


## Ingest radiosonde

1. Run Ingest

  `python3 radionsonde.py -f {name_of_file}`


## Do some studies

1. Go to the _Studies_ folder.

1. Run `jupyter notebook`

1. To perform a DB query:
  ```python3
  import graphql as gq
  query = {}
  ret = gq.make_query(query)
  ```
