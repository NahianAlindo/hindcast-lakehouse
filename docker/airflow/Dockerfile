FROM apache/airflow:3.3.0-python3.11

USER root
COPY ingestion/hindcast_extract /opt/airflow/ingestion/hindcast_extract
COPY ingestion/config /opt/airflow/ingestion/config
USER airflow

RUN pip install --no-cache-dir \
    azure-storage-blob>=12.30.0 \
    azure-identity>=1.19.0 \
    azure-keyvault-secrets>=4.9.0 \
    httpx>=0.28.1 \
    pydantic>=2.13.4 \
    python-dotenv>=1.2.2 \
    pyyaml>=6.0.3

ENV PYTHONPATH="/opt/airflow/ingestion/hindcast_extract:${PYTHONPATH}"

COPY docker/airflow/entrypoint-wrapper.sh /entrypoint-wrapper.sh
USER root
RUN chmod +x /entrypoint-wrapper.sh
USER airflow
ENTRYPOINT ["/entrypoint-wrapper.sh"]
