ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ENV AIRFLOW_HOME=/opt/airflow

# 1) On passe en root temporairement pour copier le fichier
USER root

# 2) Copier le requirements dans un répertoire où l'user airflow a accès
COPY requirements.txt ${AIRFLOW_HOME}/requirements.txt
RUN chown airflow:0 ${AIRFLOW_HOME}/requirements.txt

# 3) On repasse en user airflow pour installer les libs
USER airflow

RUN pip install --no-cache-dir -r ${AIRFLOW_HOME}/requirements.txt
