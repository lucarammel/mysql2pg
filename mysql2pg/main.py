from datetime import datetime

import yaml
from loguru import logger

from mysql2pg.main_wrapper import migrate, rename_columns, sync_tables_structure
from mysql2pg.utils import create_engine


def run_migration(
    filepath: str = "config.yaml", log_filepath: str = "logs", rename_column_option: bool = False
):
    start_time = datetime.now()
    log_file_name = f'{log_filepath}/{start_time.strftime("migration_%Y-%m-%d_%H-%M-%S.log")}'

    logger.add(log_file_name)
    with open(filepath, "r") as file:
        cfg = yaml.safe_load(file)

    pg_url = f'postgresql://{cfg["pg_username"]}:{cfg["pg_password"]}@{cfg["pg_host"]}:{cfg["pg_port"]}/{cfg["pg_database"]}'
    postgres_engine = create_engine(pg_url)

    migration_mapping = cfg["migration_mapping"]

    migrate(
        migration_mapping,
        cfg["sql_username"],
        cfg["sql_password"],
        cfg["sql_host"],
        cfg["sql_port"],
        postgres_engine,
        pg_url,
        cfg["batch_size"],
    )
    sync_tables_structure(
        migration_mapping,
        cfg["sql_username"],
        cfg["sql_password"],
        cfg["sql_host"],
        cfg["sql_port"],
        postgres_engine,
    )

    if rename_column_option:
        rename_columns(migration_mapping, postgres_engine)


if __name__ == "__main__":
    run_migration()
