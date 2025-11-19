from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import pendulum

from datawarehouse.dwh import staging_table, core_table

from api.video_stats import (
    get_paylist_id as _get_paylist_id,
    get_video_ids as _get_video_ids,
    extract_video_data as _extract_video_data,
    save_to_json as _save_to_json,
)

local_tz = pendulum.timezone("Africa/Abidjan")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["youssouf.korbeogo13@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2024, 1, 1, tzinfo=local_tz),
}

# -------- Tasks Airflow --------

@task
def get_playlist():
    # Retourne un STRING (playlist_id), sérialisable en JSON
    return _get_paylist_id()

@task
def get_videos(playlist_id: str):
    return _get_video_ids(playlist_id)

@task
def extract(videos: list[str]):
    return _extract_video_data(videos)

@task
def save(data: list[dict]):
    _save_to_json(data)


with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="Dag to produce json file",
    schedule="0 14 * * *",
    catchup=False,
) as dag:

    playlist_id = get_playlist()
    video_ids = get_videos(playlist_id)
    extracted = extract(video_ids)
    save_task = save(extracted)

    # (optionnel, TaskFlow gère déjà les dépendances via les appels)
    playlist_id >> video_ids >> extracted >> save_task


with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="Dag to process Json file and insert data into both staging and core schema",
    schedule="0 15 * * *",
    catchup=False,
) as dag:
    
    update_staging = staging_table()
    update_core = core_table()

    # (optionnel, TaskFlow gère déjà les dépendances via les appels)
    update_staging >> update_core