import logging
from airflow.decorators import task

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_loading import load_data
from datawarehouse.data_transformation import transform_data


logger = logging.getLogger(__name__)
table = "yt_api"


@task
def staging_table():
    """
    Load today's JSON into bronze.yt_api.

    - New videos are inserted.
    - Existing videos have their stats (views, likes, comments) updated.
    - Videos that disappeared from the API are deleted.
    """
    schema = 'bronze'
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()

        youtube_data = load_data()
        create_schema(schema)
        create_table(schema, table)

        existing_ids = set(get_video_ids(cursor, schema))

        for row in youtube_data:
            if row['video_id'] in existing_ids:
                update_rows(cursor, conn, schema, row)
            else:
                insert_rows(cursor, conn, schema, row)

        # Delete rows that no longer exist in the latest API pull
        json_ids = {row['video_id'] for row in youtube_data}
        ids_to_delete = existing_ids - json_ids
        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"Staging table {schema}.{table} updated successfully.")

    except Exception as e:
        logger.error(f"Error updating staging table {schema}.{table}: {e}")
        raise

    finally:
        if conn and cursor:
            close_conn_cursor(conn, cursor)


@task
def core_table():
    """
    Transform bronze rows and load them into core.yt_api.

    Reads every row from bronze, applies transform_data() to clean types
    and add video_type, then upserts into core. Videos deleted from bronze
    are also deleted from core.
    """
    schema = 'core'
    source_schema = 'bronze'
    conn, cursor = None, None

    try:
        conn, cursor = get_conn_cursor()

        create_schema(schema)
        create_table(schema, table)

        existing_core_ids = set(get_video_ids(cursor, schema))

        # Read full rows from bronze to transform
        cursor.execute(f"SELECT * FROM {source_schema}.{table};")
        bronze_rows = cursor.fetchall()

        bronze_ids = set()
        for row in bronze_rows:
            transformed_row = transform_data(row)
            bronze_ids.add(transformed_row['video_id'])

            if transformed_row['video_id'] in existing_core_ids:
                update_rows(cursor, conn, schema, transformed_row)
            else:
                insert_rows(cursor, conn, schema, transformed_row)

        # Delete from core anything no longer in bronze
        ids_to_delete = existing_core_ids - bronze_ids
        if ids_to_delete:
            delete_rows(cursor, conn, schema, ids_to_delete)

        logger.info(f"Core table {schema}.{table} updated successfully.")

    except Exception as e:
        logger.error(f"Error updating core table {schema}.{table}: {e}")
        raise

    finally:
        if conn and cursor:
            close_conn_cursor(conn, cursor)


