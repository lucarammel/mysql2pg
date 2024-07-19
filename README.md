# Migration MySQL -> PG

This script facilitates the migration of data from MySQL to PostgreSQL, ensuring the synchronization of table structures and column names, followed by data migration in batches. It handles various aspects of the migration process, including schema and table creation, data transfer, and sanity checks to verify data integrity.

## Quick start ! 

- Clone the project and get into the project: 

```bash
git clone https://gitlab.adaje.oi.enedis.fr/be-prev/migration_sql_pg.git

cd migration_sql_pg
```

- Create virtual environment : 

```bash
python -m venv venv
pip install --proxy http://vip-users.proxy.edf.fr:3131 -r requirements.txt
```

- Edit the `config.exemple.yaml` file and rename it `config.yaml`

- Run main script : 

```bash
venv/bin/python main.py
```

## How It Works

The script follows these main steps:

1. **Read Configuration**: Loads configuration parameters from `config.yaml`.
2. **Database Connection**: Creates database connections to both MySQL and PostgreSQL.
3. **Data Migration**: Migrates data from MySQL to PostgreSQL reading and uploading by batch the data.
4. **Sanity check**: The sanity check is performed at the end of the migration. A random subset of the table is downloaded in the both database and compared, The sanity check is performed five times.
5. **Structure Synchronization**: Ensures that table structures in PostgreSQL match those in MySQL. Specially the default value, the non nullability constraint and primary keys. 
6. **Column Renaming**: Renames all columns in PostgreSQL to lowercase for consistency.
