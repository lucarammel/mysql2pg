from typing import Annotated

import typer

import mysql2pg as mysql2pg
from mysql2pg.main import run_migration

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
    run_migration(filepath=filepath, log_filepath=log_filepath, rename_column_option=rename_column)


@app.command()
def version():
    print(mysql2pg.__version__)
