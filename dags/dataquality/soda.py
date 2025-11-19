import logging
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__package__)

SODA_PATH = "/opt/airflow/include/soda"
DATASOURCE = "pg_datasource"

def yt_elt_data_quality(schema):
    try:
        task = BashOperator(
            task_id=f"soda_test_{schema}", 
            bash_command=(
            f"SCHEMA={schema} "
            f"soda scan -d {DATASOURCE} "
            f"-c {SODA_PATH}/configuration.yml "
            f"{SODA_PATH}/checks.yml -V"
        )
        )
        return task
    except Exception as e:
        logger.error(f"Error running data quality check for schema: {schema}")
        raise e

        