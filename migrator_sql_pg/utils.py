import sqlalchemy as sa
import polars as pl
import pandas as pd
from loguru import logger
import time
import random
from colorama import Fore, Style, init


def create_engine(url):
    """
    Create an SQLAlchemy engine.

    Args:
        url (str): The database URL.

    Returns:
        sqlalchemy.engine.Engine: An SQLAlchemy engine.
    """
    return sa.create_engine(url)


def sync_table_structure(mysql_engine, postgresql_engine, schema, table_name):
    """
    Synchronize the table structure from a MySQL database to a PostgreSQL database.

    Args:
        mysql_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the MySQL database.
        postgresql_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the PostgreSQL database.
        schema (str): The target schema in the PostgreSQL database.
        table_name (str): The name of the table to synchronize.
    """

    mysql_query = sa.text(f"DESCRIBE {table_name}")

    pg_query = sa.text(
        f"""SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'"""
    )

    pg_query_constraints = f"""SELECT constraint_name, column_name
                            FROM information_schema.key_column_usage
                            WHERE table_schema = '{schema}'
                            AND table_name = '{table_name}'
                            AND constraint_name = (
                                SELECT constraint_name
                                FROM information_schema.table_constraints
                                WHERE table_schema = '{schema}'
                                AND table_name = '{table_name}'
                                AND constraint_type = 'PRIMARY KEY'
                            );"""

    with mysql_engine.connect() as connection:
        table_info = connection.execute(mysql_query).fetchall()
    columns_sql = [c[0] for c in table_info]

    columns_postgre = pd.read_sql(pg_query, postgresql_engine).column_name.to_list()
    pkey_postgre = pd.read_sql(
        pg_query_constraints, postgresql_engine
    ).column_name.to_list()
    is_nullable_postgre = (
        pd.read_sql(pg_query, postgresql_engine)
        .query("is_nullable == 'YES'")
        .column_name.to_list()
    )

    mapping_columns = {
        columns_sql[idx]: columns_postgre[idx] for idx in range(len(columns_sql))
    }
    columns_pk = []
    query_total_default = ""
    query_total_nullable = ""

    for column in table_info:
        column_name = column[0]
        data_type = column[1]
        is_notnull = "NOT_NULL" if column[2] == "NO" else False
        default_value = column[4] if column[4] else False
        primary_key = True if column[3] == "PRI" else False
        col_pg = mapping_columns[column_name]
        query_start = f"ALTER TABLE {schema}.{table_name} "

        query_default = ""
        if default_value:
            logger.info(f"Adding default value {default_value} for {col_pg}")
            query_default = (
                query_start
                + f"""ALTER COLUMN "{col_pg}" SET DEFAULT {default_value}; """
            )

        query_nullable = ""
        if is_notnull:
            if not col_pg in is_nullable_postgre:
                logger.info(f"Column : {col_pg} already under not nullable constraint")
            else:
                logger.info(f"Adding NOT NULL constraint for {col_pg}")
                query_nullable = (
                    query_start + f"""ALTER COLUMN "{col_pg}" SET NOT NULL; """
                )
        else:
            f"ALTER COLUMN {column_name} DROP NOT NULL; "

        if primary_key:

            if col_pg in pkey_postgre:
                logger.info(f"Column : {col_pg} already under primary key contraint")
            else:
                logger.info(f"Adding primary key for {col_pg}")
                columns_pk.append(f'"{col_pg}"')

        query_total_default += query_default
        query_total_nullable += query_nullable

    if len(columns_pk) > 0:
        query_pk = f"ALTER TABLE {schema}.{table_name} ADD PRIMARY KEY ({', '.join(columns_pk)}); "
    else:
        query_pk = ""
    query = query_total_default + query_total_nullable + query_pk
    if len(query) > 0:
        try:
            with postgresql_engine.connect() as connection:
                connection.execute(sa.text(query))
                connection.commit()
        except Exception as e:
            logger.error(e)


def check_if_table_exists(table_name, engine):
    """
    Check if a table exists in the database.

    Args:
        table_name (str): The name of the table to check.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database.

    Returns:
        int: The number of rows in the table if it exists, otherwise 0.
    """

    # Create a MetaData instance
    metadata = sa.MetaData()

    # Reflect the table from the database, if it exists
    try:
        table = sa.Table(table_name, metadata, autoload_with=engine)

        with engine.connect() as connection:
            row_count = connection.execute(
                sa.text(f"SELECT count(*) as row_count FROM {table_name}")
            ).fetchone()

        return row_count[0]
    except Exception:
        logger.warning(f"Table {table_name} does not exist in {engine}")
        return 0


def fetch_tables(engine, schema=None):
    """
    Retrieve all table names from the database.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database.
        schema (str, optional): The schema to search for tables. Defaults to None.

    Returns:
        list: A list of table names.
    """

    if schema:
        with engine.connect() as connection:
            return sa.inspect(engine).get_table_names(schema=schema)
    else:
        with engine.connect() as connection:
            return sa.inspect(engine).get_table_names()


def check_and_create_schema(engine, schema_name):
    """
    Check if a schema exists in the database and create it if it does not exist.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database.
        schema_name (str): The name of the schema to check and create if necessary.
    """

    with engine.connect() as connection:
        # Check if the schema exists
        result = connection.execute(
            sa.text(
                f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"
            ),
            {"schema": schema_name},
        ).fetchone()
        # Create the schema if it does not exist
        if not result:
            connection.execute(sa.text(f"CREATE SCHEMA {schema_name}"))
            connection.commit()
            logger.info(f"Schema '{schema_name}' created.")


def purge_schemas(engine):
    """
    Purge all user-created schemas from the PostgreSQL database.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the PostgreSQL database.
    """

    with engine.connect() as connection:
        # Start a transaction
        trans = connection.begin()
        try:
            # Retrieve all user-created schemas (excluding 'public' and system schemas)
            schemas = connection.execute(
                sa.text(
                    """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('public', 'information_schema', 'pg_catalog', 'pg_toast')
                  AND schema_name NOT LIKE 'pg_temp_%'
                  AND schema_name NOT LIKE 'pg_toast_temp_%'
            """
                )
            ).fetchall()

            for schema in schemas:
                schema_name = schema[0]
                # Drop schema with all its objects
                connection.execute(
                    sa.text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                )
                logger.info(f"Schema '{schema_name}' has been dropped.")

            # Commit transaction
            trans.commit()
        except:
            # Rollback in case of error
            trans.rollback()
            raise


def rename_columns_to_lowercase(engine, table_name, schema_name=None):
    """
    Rename all columns in a table to lowercase.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database.
        table_name (str): The name of the table.
        schema_name (str, optional): The schema name. Defaults to None.
    """

    metadata = sa.MetaData()
    metadata.reflect(bind=engine, schema=schema_name)

    table = sa.Table(table_name, metadata, schema=schema_name, autoload_with=engine)

    with engine.connect() as connection:
        # Begin a transaction
        with connection.begin():
            # Rename columns to lowercase
            for column in table.columns:
                if column.name != column.name.lower():
                    old_name = column.name
                    new_name = column.name.lower()
                    rename_column_query = sa.text(
                        f'ALTER TABLE {table.fullname} RENAME COLUMN "{old_name}" TO "{new_name}";'
                    )
                    connection.execute(rename_column_query)
                    logger.info(
                        f"Renamed column {old_name} to {new_name} for table {table_name}"
                    )


def transfer_data_in_batches(
    target_engine,
    table,
    schema,
    source_string=None,
    source_engine=None,
    target_string=None,
    batch_size=50000,
    offset_start=0,
    row_total=0,
):
    """
    Transfer data from the source database to the target database in batches.

    Args:
        target_engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the target database.
        table (str): The name of the table to transfer.
        schema (str): The schema of the target table.
        source_string (str, optional): The connection string for the source database.
        source_engine (sqlalchemy.engine.Engine, optional): The SQLAlchemy engine for the source database.
        target_string (str, optional): The connection string for the target database.
        batch_size (int, optional): The number of rows to transfer in each batch. Default is 50000.
        offset_start (int, optional): The starting offset for the transfer. Default is 0.
        row_total (int, optional): The total number of rows to transfer. Default is 0.
    """

    check_and_create_schema(target_engine, schema)

    offset = offset_start
    while True:
        # Fetch a batch of data from the source table
        query = f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}"

        logger.info(f"Table : {table}")
        start_time = time.time()
        try:
            dp = pl.read_database_uri(query, source_string)
        except Exception as e:
            logger.error("Failed to download batch")
            return None
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Read batch from MySQL in {duration:.2f}s")

        binary_columns = [
            name for name, dtype in dp.schema.items() if dtype == pl.Binary
        ]
        for col in binary_columns:
            dp = dp.with_columns(pl.col(col).cast(pl.Utf8))

        if dp.is_empty():
            logger.info(f"All done for {table} ! \n\n")
            break

        # Load the batch into the target table
        start_time = time.time()
        try:
            dp.write_database(
                f"{schema}.{table}",
                target_string,
                if_table_exists="append",
                engine="adbc",
            )
        except Exception as e:
            dp.write_database(
                f"{schema}.{table}", target_string, if_table_exists="append"
            )
            logger.warning("Had to use SQLalchemy engine instead of ADBC")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Transferred {dp.select(pl.len()).item()} rows in {duration:.2f}s")

        offset += batch_size
        logger.info(f"Progress : {min(offset/row_total, 1):.2%}\n")


def sanity_check(engine, pg_url, mysql_url_no_driver, row_count_sql, schema, table):
    logger.info("Performing sanity check ..")

    metadata = sa.MetaData()
    metadata.reflect(bind=engine, schema=schema)
    table_sql = sa.Table(table, metadata, schema=schema, autoload_with=engine)

    with engine.connect() as connection:
        # Begin a transaction
        with connection.begin():

            order_columns = [
                c.name
                for c in table_sql.columns
                if not isinstance(c.type, sa.Float)
                and not isinstance(c.type, sa.DOUBLE)
                and not isinstance(c.type, sa.DOUBLE_PRECISION)
                and not isinstance(c.type, sa.Double)
            ]

    order_by_clause = ", ".join(order_columns)

    is_equal = []
    for i in range(1, 5):
        if row_count_sql > 1e6:
            row_count_sql *= 0.1
        limit = random.randint(int(row_count_sql * 0.1), int(row_count_sql))
        offset = random.randint(0, int(row_count_sql - limit))

        query = f"SELECT * FROM {schema}.{table} ORDER BY {order_by_clause} LIMIT {limit}  OFFSET {offset}"

        try:
            dp_pg = pl.read_database_uri(query, pg_url)
            dp_sql = pl.read_database_uri(query, mysql_url_no_driver)
            rename_dict_pg = {col: col.lower() for col in dp_pg.columns}
            rename_dict_sql = {col: col.lower() for col in dp_sql.columns}

            dp_pg = dp_pg.rename(rename_dict_pg)
            dp_sql = dp_sql.rename(rename_dict_sql)
        except Exception as e:
            logger.error(e)

        is_equal.append(dp_pg.equals(dp_sql))

    if all(is_equal):
        logger.info(f"Sanity check done {Fore.GREEN}✓{Style.RESET_ALL}")
    else:
        logger.info(f"Sanity check failed {Fore.RED}✗{Style.RESET_ALL}")
