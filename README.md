# Migration MySQL -> PG

This tool facilitates the migration of data from MySQL to PostgreSQL, ensuring the synchronization of table structures and column names, followed by data migration in batches. It handles various aspects of the migration process, including schema and table creation, data transfer, and sanity checks to verify data integrity.

## Quick start ! 

- Clone the project and get into the project: 

```bash
git clone https://github.com/lucarammel/mysql2pg.git

cd mysql2pg
```

- Create virtual environment :

```bash
uv sync
```

- Edit the `config.exemple.yaml` file and rename it `config.yaml`

Use CLI to run the migration :

```bash
mysql2pg run --filepath config.yaml --log-filepath log
```

And get any helps on the CLI :

```bash
mysql2pg --help
```

![](/assets/cmd_line_mysql2pg.PNG)

The CLI is powered by **Typer** ! :rocket:

## How It Works

The script follows these main steps:

1. **Read Configuration**: Loads configuration parameters from `config.yaml`.
2. **Database Connection**: Creates database connections to both MySQL and PostgreSQL.
3. **Data Migration**: Migrates data from MySQL to PostgreSQL reading and uploading by batch the data.
4. **Sanity check**: The sanity check is performed at the end of the migration. A random subset of the table is downloaded in the both database and compared, The sanity check is performed five times.
5. **Structure Synchronization**: Ensures that table structures in PostgreSQL match those in MySQL. Specially the default value, the non nullability constraint and primary keys. 
6. **Column Renaming**: Renames all columns in PostgreSQL to lowercase for consistency.
