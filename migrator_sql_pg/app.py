import typer
from typing import Annotated
from migrator_sql_pg.main import run

app = typer.Typer()


@app.command()
def run(
    filepath: Annotated[
        str, typer.Option(help="Configuration file path. Expected format : yaml")
    ] = "config.yaml",
    log_filepath: Annotated[
        str, typer.Option(help="Log folder to write migration files")
    ] = "log",
):
    run(filepath=filepath, log_filepath=log_filepath)
