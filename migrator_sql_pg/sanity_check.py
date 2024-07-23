import sqlalchemy as sa
import polars as pl
from loguru import logger
import random
from migrator_sql_pg.retry_decorator import retry_on_failure


def sanity_check(engine, pg_url, mysql_url_no_driver, row_count_sql, schema, table):
    LOOP_SANITY = 5
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
            order_columns = [
                c.name
                for c in table_sql.columns
                if c.name in order_columns
                or ("id" in c.name and c.name not in order_columns)
            ]

    order_by_clause = ", ".join(order_columns)

    i = 1
    if row_count_sql > 1e6:
        logger.info("Sanity check by batch because too large dataset")
        while i <= LOOP_SANITY:

            limit = random.randint(int(row_count_sql * 0.1), int(row_count_sql))
            offset = random.randint(0, int(row_count_sql - limit))

            is_equal = check_is_equal(
                schema,
                table,
                order_by_clause,
                limit,
                offset,
                pg_url,
                mysql_url_no_driver,
            )

            if is_equal:
                logger.info(f"Progress sanity check : {i/LOOP_SANITY:.0%}")
                i += 1

                if i == LOOP_SANITY:
                    logger.success("Sanity check passed !")
                    return 0

            else:
                logger.warning(f"Sanity check failed")
                return 1

    else:
        is_equal = check_is_equal(
            schema,
            table,
            order_by_clause,
            row_count_sql,
            0,
            pg_url,
            mysql_url_no_driver,
        )

        if is_equal:
            logger.success("Sanity check passed !")
            return 0
        else:
            logger.warning(f"Sanity check failed")
            return 1


@retry_on_failure
def check_is_equal(
    schema, table, order_by_clause, limit, offset, pg_url, mysql_url_no_driver
):

    query = f"SELECT * FROM {schema}.{table} ORDER BY {order_by_clause} LIMIT {limit} OFFSET {offset}"

    dp_pg = pl.read_database_uri(query, pg_url)
    dp_sql = pl.read_database_uri(query, mysql_url_no_driver)
    rename_dict_pg = {col: col.lower() for col in dp_pg.columns}
    rename_dict_sql = {col: col.lower() for col in dp_sql.columns}

    dp_pg = dp_pg.rename(rename_dict_pg)
    dp_sql = dp_sql.rename(rename_dict_sql)

    is_equal = dp_pg.equals(dp_sql)

    if not is_equal:
        logger.debug(f"Query : {query}")
        logger.debug(f"PostgreSQL : {dp_pg}")
        logger.debug(f"MySQL : {dp_sql}")
    
    return is_equal
