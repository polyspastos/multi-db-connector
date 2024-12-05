# Multi-database connector
connect to postgres and sqlite, populate, run queries, inspect data, do analyses and plots

## Setup - Windows
1. download and install postgres from https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
2. create virtual environment
```
python -m venv venv
```
3. activate virtual environment
```
.\venv\Scripts\activate
```
4. install dependencies
```
pip install -r requirements.txt
```
5. put postgres directory in your path
```
set PATH=C:\Program Files\PostgreSQL\17\bin;%PATH%
```

## Setup - Linux
1. install postgres
```
sudo apt update
sudo apt install postgresql postgresql-contrib
```
2. create virtual environment
```
python -m venv venv
```
3. activate virtual environment
```
source venv/bin/activate
```
4. install dependencies
```
pip install -r requirements.txt
```

## Usage

### Cleanup
```
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders;
```

or use
```
python -c "from src.db_connector import DatabaseConnector; DatabaseConnector().cleanup_databases()"
```

care that based on current working directory and your virtual environment setup, you might need to change the relative import of the dotenv

### Run scripts in this order
setup tables, populate, test, verify
```
python src/setup_and_test_queries.py
```

build up to a join and stats
```
python src/verify_data.py
```

complex queries and stats
```
python src/complex_queries.py
```


## Inspect data
### a. postgres
#### i. psql
```
\c <db_name>
\dt
\d orders
\q
```
#### ii. pgadmin
download and install pgadmin from https://www.pgadmin.org/download/

### b. sqlite
download and install browser from https://sqlitebrowser.org/ 
