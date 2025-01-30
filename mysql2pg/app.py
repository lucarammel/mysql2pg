from typing import Annotated

import typer
import yaml
from urllib.parse import quote_plus
from datetime import datetime

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
    log_filepath: Annotated[
        str, typer.Option(help="Log folder to write migration files")
    ] = "logs",
):
    """
    Purge a PostgreSQL database.
    """
    start_time = datetime.now()
    log_file_name = (
        f'{log_filepath}/{start_time.strftime("migration_%Y-%m-%d_%H-%M-%S.log")}'
    )
    
    with open(filepath, "r") as file:
        cfg = yaml.safe_load(file)

    encoded_password = quote_plus(cfg["pg_password"])
    pg_url = f'postgresql://{cfg["pg_username"]}:{encoded_password}@{cfg["pg_host"]}:{cfg["pg_port"]}/{cfg["pg_database"]}'
    postgres_engine = create_engine(pg_url)
    purge_schemas(postgres_engine)
