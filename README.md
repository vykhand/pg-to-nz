# Package pg-to-nz: PostgreSQL to Netezza (PureData for Analytics) database migration

This is a simple python package that I wrote to do one time data movement from PostgreSQL to Netezza

## How it works

1. Migrates DDL from PostgreSQL to Netezza
  
  ```
  import pg_to_nz.DBMigrator as dbm
  mg = dbm.DBMigrator()
  mg.migrate_ddl(drop_table = True, raise_error=False)
  ```
2. Migrates data. Downloads PostgreSQL tables into data/ directory with COPY command and uploads to Netezza using nzload command
  
  ```
  mg.migrate_data(trunc_tables=True, overwrite_files=False)
  ```

## Requirements and dependencies

This was tested and developed on Windows 7 64 bit workstations. Any other environments will require modifications and additional testing.

You need to have Netezza ODBC driver installed and Netezza Tools (nzload)

Databases tested: PostreSQL 9.2, Netezza NPS 7.2

Requires the following python packages:
  * pyodbc
  * psycopg2
  * pyyaml

## Settings

all settings are located in config.yml. Classes don't take parameters, everything is configured via file.

  * pg_host: your postgres hostname or IP
  * pg_db:  your postgres database name
  * pg_user: your postgres user name
  * pg_pwd: your postgres password
  * pg_port: 5432
  * pg_schema: public
  * nz_db: your nz database name
  * nz_user: your nz user name
  * nz_pwd: your nz password
  * nz_client_dir: C:/Program Files (x86)/IBM Netezza Tools/Bin
  * nz_driver: NetezzaSQL
  * nz_server: nz IP address or hostname



## Usage

```
git clone https://github.com/vykhand/pg-to-nz
# configure config.yml
python migrate.py
# pray that it works. If you are not a religious person, just cross your fingers.
```

If it does not work as designed, try to fix it.




