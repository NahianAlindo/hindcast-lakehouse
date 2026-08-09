FROM python:3.11-slim

# This base image's Debian release only ships openjdk-21/25, not 17 (which
# WSL2's Ubuntu 24.04 has, and is what local dev in Step 1 used). Spark 3.5.x
# added Java 21 support (SPARK-43831); verified empirically here that
# pyspark 3.5.9 actually boots and runs correctly under it before trusting
# that claim on this specific base image, not just assumed compatible.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Exact versions verified against Maven Central's actual dependency
# metadata, not guessed -- see transform/spark/jobs/spark_session.py's
# docstring for the full reasoning (pyspark 3.5.9 bundles Hadoop 3.3.4,
# whose own POM pins azure-storage to 7.0.1; the three Delta jars are what
# configure_spark_with_delta_pip resolves for delta-spark==3.2.1).
WORKDIR /opt/spark-jars
RUN curl -sL -o hadoop-azure-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    && curl -sL -o azure-storage-7.0.1.jar \
      https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar \
    && curl -sL -o io.delta_delta-spark_2.12-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    && curl -sL -o io.delta_delta-storage-3.2.1.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    && curl -sL -o org.antlr_antlr4-runtime-4.9.3.jar \
      https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

RUN pip install --no-cache-dir --upgrade pip

# pyspark ships no wheel on PyPI, only an sdist (verified against PyPI's own
# JSON API before assuming this, not guessed) -- --only-binary :all: has
# nothing to select for it and fails outright. Its "build" is pure Python
# packaging (bundling the JARs pip.py already has locally), not compiling
# untrusted C/Rust, so it's the one package here that's safe to exempt
# rather than a real instance of the setup-script-execution risk that flag
# guards against everywhere else. Installed first and on its own: delta-spark
# also declares a transitive pyspark<3.6.0,>=3.5.3 dependency, and pip
# re-resolves that under --only-binary in the same command if pyspark isn't
# already satisfied first.
RUN pip install --no-cache-dir pyspark==3.5.9
RUN pip install --no-cache-dir --only-binary :all: \
    azure-identity==1.25.3 \
    azure-keyvault-secrets==4.11.0 \
    delta-spark==3.2.1 \
    "pandas>=2.3.3,<3" \
    "pandera[pyspark]>=0.32.1"

WORKDIR /opt/transform
COPY transform/spark/jobs /opt/transform/jobs

ENV SPARK_ADLS_JARS_DIR=/opt/spark-jars
ENV SPARK_MASTER=local[*]
ENV PYTHONPATH=/opt/transform/jobs

COPY docker/spark/entrypoint-wrapper.sh /entrypoint-wrapper.sh
RUN chmod +x /entrypoint-wrapper.sh
ENTRYPOINT ["/entrypoint-wrapper.sh"]
