from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
import logging


table = 'yt_api'


logger = logging.getLogger(__name__)

def get_conn_cursor():
    """
    Returns a tuple (conn, cur) for interacting with Postgres.
    
    This helper ensures:
      - A valid connection is created using the Airflow connection ID.
      - The cursor uses RealDictCursor (dict-like rows).
      - Any connection failure raises a clear exception so the task properly fails.

    NOTE:
      • 'schema' is the correct argument for PostgresHook.
      • The parameter 'database' is ignored and should not be used.
    """
    try:
        # Build the Postgres connection based on the Airflow connection ID.
        hook = PostgresHook(
            postgres_conn_id="postgres_db_yt_elt",
            schema="elt_db"   # Specifies the target database/schema
        )
        
        # Actual psycopg2 connection object.
        conn = hook.get_conn()
        
        # Cursor that returns rows as Python dictionaries instead of tuples.
        cur = conn.cursor(cursor_factory=RealDictCursor)

        logger.info("Successfully established connection to Postgres.")
        return conn, cur

    except Exception as e:
        # Log the full stack trace for debugging inside Airflow logs.
        logger.exception("Failed to establish Postgres connection or cursor.")
        raise


def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):
    
    conn, cur = get_conn_cursor()
    
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    cur.execute(schema_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
                );
                """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
                );
                """
    
    cur.execute(table_sql)

    conn.commit()

    close_conn_cursor(conn, cur)

def get_video_ids(schema, cur):

    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    ids = cur.fetchall()
    video_id = [row['Video_ID'] for row in ids]

    return video_id





