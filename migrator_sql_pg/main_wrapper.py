from loguru import logger
from migrator_sql_pg.utils import (
    create_engine,
    rename_columns_to_lowercase,
    check_if_table_exists,
    fetch_tables,
    transfer_data_in_batches,
    sync_table_structure,
    sanity_check,
)


def migrate(
    to_migrate,
    sql_username,
    sql_password,
    sql_host,
    sql_port,
    postgres_engine,
    pg_url,
    batch_size,
):
    """
    Migrate data from MySQL to PostgreSQL for the specified schemas and tables.

    Args:
        to_migrate (dict): A dictionary specifying the schemas and tables to migrate.
        sql_username (str): The MySQL username.
        sql_password (str): The MySQL password.
        sql_host (str): The MySQL host.
        sql_port (int): The MySQL port.
        postgres_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the PostgreSQL database.
    """

    for schema in to_migrate.keys():
        logger.info(f"*********** Schema set to {schema} ************ \n")
        # SQLAlchemy database URL
        mysql_url = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{schema}"
        sql_url_no_driver = (
            f"mysql://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{schema}"
        )

        # Create SQLAlchemy engine
        sql_engine = create_engine(mysql_url)

        # Retrieve all tables
        tables = fetch_tables(sql_engine)
        tables = (
            [t for t in tables if t in to_migrate[schema]]
            if to_migrate[schema] != ["all"]
            else tables
        )

        # Retrieve and logger.info data from each table using Polars
        for idx, table in enumerate(tables):
            try:
                logger.info(f"Migrating {table} ..")
                row_count_sql = check_if_table_exists(table, sql_engine)
                row_count_pg = check_if_table_exists(table, postgres_engine)

                if row_count_sql > row_count_pg:
                    offset_start = row_count_pg
                    logger.info(
                        f"Table : {table} - row count SQL vs PG : {row_count_sql} <-> {row_count_pg}. \n"
                    )
                    logger.info(f"Starting migration with offset {offset_start}")

                    transfer_data_in_batches(
                        source_string=sql_url_no_driver,
                        target_engine=postgres_engine,
                        target_string=pg_url,
                        table=table,
                        source_engine=sql_engine,
                        schema=schema,
                        batch_size=batch_size,
                        offset_start=offset_start,
                        row_total=row_count_sql,
                    )

                    sanity_check(
                        postgres_engine,
                        pg_url,
                        sql_url_no_driver,
                        row_count_sql,
                        schema,
                        table,
                    )
                    logger.success(f"Migration done for {table}")
                else:
                    sanity_check(
                        postgres_engine,
                        pg_url,
                        sql_url_no_driver,
                        row_count_sql,
                        schema,
                        table,
                    )

                    logger.success(f"Migration already done for {table}")

            except Exception as e:
                # purge_schemas(postgres_engine)
                logger.error(e)

            logger.info(
                f"Avancement of schema processed : {(idx+1)/len(tables):.0%} \n\n"
            )


def rename_columns(to_migrate, postgres_engine):
    """
    Rename all columns to lowercase for the specified schemas in PostgreSQL.

    Args:
        to_migrate (dict): A dictionary specifying the schemas to rename columns for.
        postgres_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the PostgreSQL database.
    """

    for schema in to_migrate.keys():
        logger.info(f"*********** Schema set to {schema} ************ \n")
        tables = fetch_tables(postgres_engine, schema=schema)
        for table in tables:
            rename_columns_to_lowercase(postgres_engine, table, schema)


def sync_tables_structure(
    to_migrate, sql_username, sql_password, sql_host, sql_port, postgres_engine
):
    """
    Synchronize the table structure from MySQL to PostgreSQL for the specified schemas and tables.

    Args:
        to_migrate (dict): A dictionary specifying the schemas and tables to synchronize.
        sql_username (str): The MySQL username.
        sql_password (str): The MySQL password.
        sql_host (str): The MySQL host.
        sql_port (int): The MySQL port.
        postgres_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the PostgreSQL database.
    """

    for schema in to_migrate.keys():
        logger.info(f"*********** Schema set to {schema} ************ \n")
        # SQLAlchemy database URL
        mysql_url = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{schema}"

        # Create SQLAlchemy engine
        sql_engine = create_engine(mysql_url)

        try:
            # Retrieve all tables
            tables = fetch_tables(postgres_engine, schema=schema)
            tables = (
                [t for t in tables if t in to_migrate[schema]]
                if to_migrate[schema] != ["all"]
                else tables
            )

            # Retrieve and logger.info data from each table using Polars
            for table in tables:
                row_count_sql = check_if_table_exists(table, sql_engine)
                row_count_pg = check_if_table_exists(table, postgres_engine)
                if row_count_pg * row_count_sql > 0:
                    sync_table_structure(sql_engine, postgres_engine, schema, table)
        except Exception as e:
            logger.error(e)
