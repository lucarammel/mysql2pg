from typing import Annotated

import typer
import yaml

import mysql2pg as mysql2pg
from mysql2pg.main import run_migration
from mysql2pg.utils import create_engine, purge_schemas

app = typer.Typer()


@app.command()
def run(
    filepath: Annotated[
        str, typer.Option(help="Configuration file path. Expected format : yaml")
    ] = "config.yaml",
    log_filepath: Annotated[str, typer.Option(help="Log folder to write migration files")] = "log",
    rename_column: Annotated[
        bool, typer.Option(help="Whether to rename columns in lowercase")
    ] = False,
):
    """
    Run a migration from MySQL to PGSql based on a config file.
    """
    run_migration(filepath=filepath, log_filepath=log_filepath, rename_column_option=rename_column)


@app.command()
def version():
    """
    Package version
    """
    print(mysql2pg.__version__)


@app.command()
def purge_db(
    filepath: Annotated[
        str, typer.Option(help="Configuration file path. Expected format : yaml")
    ] = "config.yaml",
):
    """
    Purge a PostgreSQL database.
    """
    with open(filepath, "r") as file:
        cfg = yaml.safe_load(file)

    pg_url = f'postgresql://{cfg["pg_username"]}:{cfg["pg_password"]}@{cfg["pg_host"]}:{cfg["pg_port"]}/{cfg["pg_database"]}'
    postgres_engine = create_engine(pg_url)
    purge_schemas(postgres_engine)
