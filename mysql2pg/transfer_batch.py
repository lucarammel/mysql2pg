from mysql2pg.utils import check_and_create_schema
import time
from loguru import logger
import polars as pl
from mysql2pg.retry_decorator import retry_on_failure


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

        dp = download_batch(query, source_string)
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Read batch from MySQL in {duration:.2f}s")

        binary_columns = [
            name for name, dtype in dp.schema.items() if dtype == pl.Binary
        ]
        for col in binary_columns:
            dp = dp.with_columns(pl.col(col).cast(pl.Utf8))

        if dp.is_empty():
            logger.success(f"Data migration done for {table} ! \n")
            break

        # lower case
        rename_dict = {col: col.lower() for col in dp.columns}
        dp = dp.rename(rename_dict)

        # Load the batch into the target table
        start_time = time.time()

        transfer_batch(dp, schema, table, target_string, offset)

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Transferred {dp.select(pl.len()).item()} rows in {duration:.2f}s")

        offset += batch_size
        logger.info(f"Progress : {min(offset/row_total, 1):.2%}\n")


@retry_on_failure
def download_batch(query, source_string):
    dp = pl.read_database_uri(query, source_string)
    return dp


def transfer_batch(dp, schema, table, target_string, offset):

    try:
        if offset == 0:
            dp.write_database(
                f"{schema}.{table}",
                target_string,
                engine="adbc",
            )
        else:
            dp.write_database(
                f"{schema}.{table}",
                target_string,
                if_table_exists="append",
                engine="adbc",
            )
    except Exception as e:
        if offset == 0:
            dp.write_database(f"{schema}.{table}", target_string)
        else:
            dp.write_database(
                f"{schema}.{table}", target_string, if_table_exists="append"
            )
        logger.warning("Had to use SQLalchemy engine instead of ADBC")
