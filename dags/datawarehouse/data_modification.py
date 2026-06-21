import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cursor, conn, schema, row):
    """
    Insert a single row into the given schema's table.

    Bronze rows come from the JSON file so the title key is 'title'.
    Core rows come from the bronze table so the title key is 'video_title'.
    ON CONFLICT DO NOTHING avoids crashing if the row already exists —
    updates are handled separately by update_rows().
    """
    try:
        if schema == 'bronze':
            cursor.execute(f"""
                INSERT INTO {schema}.{table}
                    (video_id, video_title, published_at, duration, view_count, like_count, comment_count)
                VALUES
                    (%(video_id)s, %(title)s, %(published_at)s, %(duration)s,
                     %(view_count)s, %(like_count)s, %(comment_count)s)
                ON CONFLICT (video_id) DO NOTHING;
            """, row)
        else:
            cursor.execute(f"""
                INSERT INTO {schema}.{table}
                    (video_id, video_title, published_at, duration, video_type, view_count, like_count, comment_count)
                VALUES
                    (%(video_id)s, %(video_title)s, %(published_at)s, %(duration)s, %(video_type)s,
                     %(view_count)s, %(like_count)s, %(comment_count)s)
                ON CONFLICT (video_id) DO NOTHING;
            """, row)

        conn.commit()
        logger.info(f"Inserted video_id {row['video_id']} into {schema}.{table}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error inserting video_id {row['video_id']} into {schema}.{table}: {e}")
        raise


def update_rows(cursor, conn, schema, row):
    """
    Update stats for an existing row. Only columns that change over time are updated
    (views, likes, comments, title). published_at and video_id never change.
    """
    try:
        if schema == 'bronze':
            cursor.execute(f"""
                UPDATE {schema}.{table}
                SET video_title   = %(title)s,
                    view_count    = %(view_count)s,
                    like_count    = %(like_count)s,
                    comment_count = %(comment_count)s
                WHERE video_id = %(video_id)s;
            """, row)
        else:
            cursor.execute(f"""
                UPDATE {schema}.{table}
                SET video_title   = %(video_title)s,
                    view_count    = %(view_count)s,
                    like_count    = %(like_count)s,
                    comment_count = %(comment_count)s,
                    video_type    = %(video_type)s
                WHERE video_id = %(video_id)s;
            """, row)

        conn.commit()
        logger.info(f"Updated video_id {row['video_id']} in {schema}.{table}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating video_id {row['video_id']} in {schema}.{table}: {e}")
        raise


def delete_rows(cursor, conn, schema, ids_to_delete):
    """
    Delete rows whose video_ids no longer exist in the source data.
    Uses ANY(%s) with a list parameter to avoid SQL injection.
    """
    try:
        cursor.execute(
            f"DELETE FROM {schema}.{table} WHERE video_id = ANY(%s);",
            (list(ids_to_delete),)
        )
        conn.commit()
        logger.info(f"Deleted {len(ids_to_delete)} rows from {schema}.{table}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting rows from {schema}.{table}: {e}")
        raise
