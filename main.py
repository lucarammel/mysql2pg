import yaml
from migrator_sql_pg.utils import create_engine
from migrator_sql_pg.main_wrapper import sync_tables_structure, rename_columns, migrate

if __name__ == "__main__":

    with open("config.yaml", "r") as file:
        cfg = yaml.safe_load(file)

    pg_url = f'postgresql://{cfg["pg_username"]}:{cfg["pg_password"]}@{cfg["pg_host"]}:{cfg["pg_port"]}/{cfg["pg_database"]}'
    postgres_engine = create_engine(pg_url)

    to_migrate = cfg["to_migrate"]

    migrate(
        to_migrate,
        cfg["sql_username"],
        cfg["sql_password"],
        cfg["sql_host"],
        cfg["sql_port"],
        postgres_engine,
        pg_url,
    )
    sync_tables_structure(
        to_migrate,
        cfg["sql_username"],
        cfg["sql_password"],
        cfg["sql_host"],
        cfg["sql_port"],
        postgres_engine,
    )
    rename_columns(to_migrate, postgres_engine)
