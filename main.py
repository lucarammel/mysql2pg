import yaml
from loguru import logger
from datetime import datetime
from migrator_sql_pg.utils import create_engine
from migrator_sql_pg.main_wrapper import sync_tables_structure, migrate

if __name__ == "__main__":

    start_time = datetime.now()
    log_file_name = f'logs/{start_time.strftime("migration_%Y-%m-%d_%H-%M-%S.log")}'

    logger.add(log_file_name)
    with open("config.yaml", "r") as file:
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
