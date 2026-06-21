from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"


def get_conn_cursor():
    """Open a connection and cursor to the ELT PostgreSQL database."""
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor


def close_conn_cursor(conn, cursor):
    """Close the cursor and connection."""
    cursor.close()
    conn.close()


def create_schema(schema):
    """Create a schema if it does not already exist."""
    conn, cursor = get_conn_cursor()
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    conn.commit()
    close_conn_cursor(conn, cursor)


def create_table(schema, table):
    """
    Create the yt_api table in the given schema if it does not already exist.

    Bronze stores everything as raw strings exactly as the API returned them.
    Core stores cleaned and typed data, including a computed video_type column.
    """
    conn, cursor = get_conn_cursor()

    if schema == 'bronze':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id      VARCHAR(11)  PRIMARY KEY NOT NULL,
                video_title   TEXT,
                published_at  VARCHAR(30),
                duration      VARCHAR(20),
                view_count    VARCHAR(20),
                like_count    VARCHAR(20),
                comment_count VARCHAR(20)
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id      VARCHAR(11)  PRIMARY KEY NOT NULL,
                video_title   TEXT,
                published_at  TIMESTAMP,
                duration      VARCHAR(10),
                video_type    VARCHAR(10),
                view_count    BIGINT,
                like_count    BIGINT,
                comment_count BIGINT
            );
        """

    cursor.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)


def get_video_ids(cursor, schema):
    """Return a list of all video_ids currently in the given schema's table."""
    cursor.execute(f"SELECT video_id FROM {schema}.{table};")
    rows = cursor.fetchall()
    return [row['video_id'] for row in rows]
