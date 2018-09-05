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

- Start the service by `python3 main.py`