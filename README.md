# The LogClick Archiving Service

This service objective is to move & purge `log_clicks` in Hermo's Production Database.
It will take last 7 days data and store in the archive database, and will purge data in Production Database.

### Starting Up

- Activate the python environment in the current directory into your preferred `env` dir

```bash
virtualenv env
```

- Install all the dependencies

```
pip install requirements.txt
```

- Setup the config. In order to deploy to production server, please create the file `config-production.ini` at the same directory level of `main.py`
- Fill up the file with this content

```
[STATE]
DATABASE = DEVELOPMENT ENVIRONMENT
SYNC_DELTA_DAYS = 7

[ORIGIN]
HOST = localhost
PORT = 0000
DATABASE = origin_db_name
USER = username
PASSWORD = password
TABLE = origin_table 

[ARCHIVE]
HOST = localhost
PORT = 0000
DATABASE = archive_db_name
USER = username
PASSWORD = password
TABLE = archive_table
```

- Then you can start the service by `python3 main.py`