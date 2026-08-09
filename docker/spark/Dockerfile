# syntax=docker/dockerfile:1
FROM python:3.11-slim

# This base image's Debian release only ships openjdk-21/25, not 17 (which
# WSL2's Ubuntu 24.04 has, and is what local dev in Step 1 used). Spark 3.5.x
# added Java 21 support (SPARK-43831); verified empirically here that
# pyspark 3.5.9 actually boots and runs correctly under it before trusting
# that claim on this specific base image, not just assumed compatible.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Exact versions verified against Maven Central's actual dependency
# metadata, not guessed -- see transform/spark/jobs/spark_session.py's
# docstring for the full reasoning (pyspark 3.5.9 bundles Hadoop 3.3.4,
# whose own POM pins azure-storage to 7.0.1; the three Delta jars are what
# configure_spark_with_delta_pip resolves for delta-spark==3.2.1). ADD (not
# RUN curl) so BuildKit verifies each download against a checksum computed
# from the actual artifact -- a substituted/tampered jar fails the build
# instead of silently installing.
WORKDIR /opt/spark-jars
ADD --checksum=sha256:37adcc23d4db068834acf2b21b6931b609a43cf0a9b4926c440f472af7abad01 \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \
    hadoop-azure-3.3.4.jar
ADD --checksum=sha256:32249cf9b550e247e257c0a508f93526d35e072509aa637fd33b3d54e267b1e2 \
    https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/7.0.1/azure-storage-7.0.1.jar \
    azure-storage-7.0.1.jar
ADD --checksum=sha256:088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499 \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
    io.delta_delta-spark_2.12-3.2.1.jar
ADD --checksum=sha256:4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7 \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
    io.delta_delta-storage-3.2.1.jar
ADD --checksum=sha256:131a6594969bc4f321d652ea2a33bc0e378ca312685ef87791b2c60b29d01ea5 \
    https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
    org.antlr_antlr4-runtime-4.9.3.jar

# pyspark ships no wheel on PyPI, only an sdist (verified against PyPI's own
# JSON API before assuming this, not guessed) -- --only-binary :all: has
# nothing to select for it and fails outright. Its "build" is pure Python
# packaging (bundling the JARs pip.py already has locally), not compiling
# untrusted C/Rust, so it's the one package here that's safe to exempt
# rather than a real instance of the setup-script-execution risk that flag
# guards against everywhere else. Installed first and on its own: delta-spark
# also declares a transitive pyspark<3.6.0,>=3.5.3 dependency, and pip
# re-resolves that under --only-binary in the same command if pyspark isn't
# already satisfied first. Chained with `&&` into one RUN (fewer layers)
# while keeping the two `pip install` invocations separate, since that
# separation is what makes the exemption above actually work -- combining
# them into a single invocation would put pyspark back under
# --only-binary too and fail the exact way it did when this was first built.
RUN pip install --no-cache-dir --only-binary :all: pip==25.2 \
    && pip install --no-cache-dir pyspark==3.5.9 \
    && pip install --no-cache-dir --only-binary :all: \
       azure-identity==1.25.3 \
       azure-keyvault-secrets==4.11.0 \
       delta-spark==3.2.1 \
       pandas==2.3.3 \
       pandera[pyspark]==0.32.1

WORKDIR /opt/transform
COPY transform/spark/jobs /opt/transform/jobs

ENV SPARK_ADLS_JARS_DIR=/opt/spark-jars
ENV SPARK_MASTER=local[*]
ENV PYTHONPATH=/opt/transform/jobs

COPY docker/spark/entrypoint-wrapper.sh /entrypoint-wrapper.sh
RUN chmod +x /entrypoint-wrapper.sh

# Runs as a dedicated non-root user -- the base python image defaults to
# root, and nothing this container does (fetching a secret over HTTPS,
# running a short-lived Spark job, exiting) needs elevated privileges.
RUN useradd --create-home --uid 1000 sparkuser \
    && chown -R sparkuser:sparkuser /opt/spark-jars /opt/transform
USER sparkuser

ENTRYPOINT ["/entrypoint-wrapper.sh"]
