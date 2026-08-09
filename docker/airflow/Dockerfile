FROM apache/airflow:3.3.0-python3.11

USER root
COPY ingestion/hindcast_extract /opt/airflow/ingestion/hindcast_extract
COPY ingestion/config /opt/airflow/ingestion/config
USER airflow

# Exact pins (matching uv.lock's resolved versions where this package is
# also a project dependency) + --only-binary :all: so this can only ever
# install prebuilt wheels, never build from an sdist -- which would mean
# running that package's arbitrary setup.py/build backend at image-build
# time.
RUN pip install --no-cache-dir --only-binary :all: \
    azure-identity==1.25.3 \
    azure-keyvault-secrets==4.11.0 \
    azure-storage-blob==12.30.0 \
    httpx==0.28.1 \
    pydantic==2.13.4 \
    python-dotenv==1.2.2 \
    pyyaml==6.0.3

ENV PYTHONPATH="/opt/airflow/ingestion/hindcast_extract:${PYTHONPATH}"

COPY docker/airflow/entrypoint-wrapper.sh /entrypoint-wrapper.sh
USER root
RUN chmod +x /entrypoint-wrapper.sh
USER airflow
ENTRYPOINT ["/entrypoint-wrapper.sh"]
