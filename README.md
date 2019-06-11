### Climate of the Boreal

__Here lies code for my blog at https://climateoftheboreal.blogspot.com__

Within this hallowed hall of digital ones and zeroes is code that allows users to ingest and parse some weather data.
I hope that the docs and infrastructure are solid enough so that others can replicate my work if they wish and even improve on it.

Within _deprecated_ lies the first attempt that has died. While it was working okay, I really want to get to know other technologies like graphql which I think will lead to a better experience - or at least a less boring one. I also want better code and doc organization. So to accomplish this, I'm starting out again. By using things that I've learned over the last few years or so at work, I hope to create a better project for myself and for those who read this repo.  

Within _code_ is where the data ingest and storage happens. Earth data (like radiosondes measurements) are parsed and ingested into a postrges DB for later use. Perhaps some basic satellite products are looked at. If something is suppose to be ran in another platform like AWS (and not on the local desktop), then files like cloudformation will be provided. Someone with little programming knowledge should be able to understand and reproduce my results without too much effort.

Within _studies_ is where where the code needed to actually produce the numbers and to graph plots for a particular study are held. Also included will be the text report the will usually become the blog post. I've decided that Jupyter notebooks are the best way to capture my ideas. Users can spin up a local notebook server and experiment with the code.

Within _lib_ is some common code that doesn't belong solely in _studies_ or _code_.


## Prerequisites and setup

**The following directions assume an Ubuntu working environment. Individual adaption might needs apply.**

To setup for ingesting and production, you will need a good computer. I expect that most people who read this already have a powerful enough computer somewhere.

#### A. The current setup uses the following technologies:

1. Python. It's assumed that Python is already installed but it's existence is worth noting.

1. [Docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

    (Note for Mac and Windows that if you don't want to sign up and don't mind a version that might not work: https://docs.docker.com/release-notes/)

1. [Hasura](https://docs.hasura.io/1.0/graphql/manual/getting-started/docker-simple.html)

    Hasura is a graphql engine that interfaces with postgres. We will be using Harusa for handling the graphql calls and for initial data exploration.

1. [Pipenv](https://github.com/pypa/pipenv)

    While it does have it's flaws, `pipenv` does make creating, starting and stopping a virtual environment easy. To be aware that the virtual shell is based off the residing directory and so you really only have pipenv to work wih.

    While there is many ways to install `pipenv`, I have found the following to work for me.

    ```bash
    pip3 install --user pipenv
    export PATH="${HOME}/.local/bin:$PATH"
    ```

#### B. Python paths

1. Add _lib_ to the `PYTHONPATH`. Append to `~/.bashrc` the following:

  `export PYTHONPATH=$PYTHONPATH:$HOME/GIT/climateoftheboreal/lib/`

  or whaterver path is appropriate.

1. To make it effective, `source ~/.bashrc`.


#### C. Pipenv

1. After getting `pipenv` installed, from this repo start `pipenv shell`.

  > To start fresh, `pipenv install requests jupyter pandas altair vega_datasets vega altair_recipes`.

  > To remove `pipenv`, `pipenv --rm`

1. After installing and running Docker, spin up the Hasura engine and Postgres.
To do so, within the root of the repo: `docker-compose up -d`

  > If starting from scratch and not using what is in the Github repo, download the script https://raw.githubusercontent.com/hasura/graphql-engine/master/install-manifests/docker-compose/docker-compose.yaml and replace `service.postgres.volumes` with `{$PWD}/code/data/db_sym/:/var/lib/postgresql/`. Make sure that `pwd` is the root of the repo.

1. Check to make sure Hasura and Postrges is running via `docker ps`.

  Hasura's UI can be found at `http://localhost:8080/console`

  > To shutdown the servers: `docker-compose down`

## Library Helps

To help with data analysis, a graphql wrapper can be used to retrieve data.

From within any script or notebook within a running shell, use `import graphql as gq`. If the import doesn't work, check that the `PYTHONPATH` was set properly as above.   

And example of using the `graphql` method:

```python3
import graphql as gq

query = """
{
  levels(order_by: {
    header: {
      year: asc, month: asc, day: asc, hour: asc}
    },
    where: {
      lvltyp1: {_eq: 1}
    }
  ) {
    wdir
    wspd
    header {
      year
      month
      day
      hour
    }
  }
}
"""

df = gq.query(query, top='levels')
```

## Data

### Radiosonde

Radiosonde data is weather balloon data. Launched twice a day at the same time at locations around the world, these _in situ_ probes take measurements of temperature, pressure, winds, and humility from the surface of the Earth to 10+ miles well into the stratosphere.

##### Download data

The studies in this repo are designed to use the NOAA archive at https://www.ncdc.noaa.gov/data-access/weather-balloon/integrated-global-radiosonde-archive. while there are other radiosonde products (paid and otherwise) to could be used, I felt the need for data that was easy to get and ingest.

1. At http://www1.ncdc.noaa.gov/pub/data/igra/igra2-station-list.txt we can find the station ID desired. For instance, for Fairbanks we have the entry

  `USM00070261  64.8161 -147.8767  133.7 AK FAIRBANKS/INT.                 1930 2019  66329`

  where the station id is `00070261`.

1. With the station id we can get the actual data at https://www1.ncdc.noaa.gov/pub/data/igra/. There are a few choices including
  - Use *data/data_por/* for all data.
  - Use *data/data_y2d/* for all data since beginning of the current year.

1. After downloading the file(s), place in the _data_ directory and unzip.

##### Ingest Data

1. To ingest the data into Postgres DB, make sure that the Postgres DB is running via `docker-compose`. Also make sure that the `pipenv` shell is running.

  > Previous data will not be overwritten. If the ingest is reran with the same data, primary key violations will happen. Make sure that any previous data is removed. It is hoped that any future ingest pipeline will take this into account.

  > Historical note: The first iteration of the code ingested data linearly. This would have taken about 5 hours. Using multiprocessing, this was reduced to 1.5 hours. Using postgres' `COPY` method, this has been reduced to 5 minutes!

1. Within Hasura, go to the SQL section and run the following SQL statements manually:

  > This is only needed if not ran before.

  > It's important that the table fields be the particular order that they are at.

  ```SQL
  CREATE TABLE header (
        id TEXT,
        year INT,
        month INT,
        day INT,
        hour INT,
        reltime INT,
        numlev INT,
        p_src TEXT,
        np_src TEXT,
        lat INT,
        lon INT,
        header_id TEXT PRIMARY KEY
    );

    CREATE TABLE
    levels (
        level_id SERIAL PRIMARY KEY,
        header_id TEXT REFERENCES header(header_id),
        lvltyp1 INT,
        lvltyp2 INT,
        etime INT,
        press INT,
        pflag TEXT,
        gph INT,
        zflag TEXT,
        temp INT,
        tflag TEXT,
        rh INT,
        dpdp INT,
        wdir INT,
        wspd INT
    );
  ```

1. Within the _code_ directory, run `python3 radionsonde.py -f {name_of_radiosonde_file}`.

1. The results of the parsing will be divded into two files: `header.csv` and `levels.csv`.

1. Move these files to the `data/db_sym` directory. Using `sudo` is likely needed.

  `sudo cp header.csv data/db_sym/header.csv`
  `sudo cp levels.csv data/dvb_sym/levels.csv`

1. From with the SQL section of Hasura, run the following commands. They will take about 2 seconds and 1.5 minutes, respectively.

  ```POSTGRES
  COPY header(id, year, month, day, hour, reltime, numlev, p_src, np_src, lat, lon, header_id) from '/var/lib/postgresql/header.csv' NULL as 'null' CSV;

  COPY levels(header_id, lvltyp1, lvltyp2, etime, press, pflag, gph, zflag, temp, tflag, rh, dpdp, wdir, wspd) from '/var/lib/postgresql/levels.csv' NULL as 'null' CSV;
  ```

  We are taking advantage of the docker volume mount created for persisting the Postgres data.

1. Running SQL queries should return data.
