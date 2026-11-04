FROM apache/airflow:2.10.5-python3.11

USER root

# Java (necessário para o Spark) + curl
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/wecogno-java

ENV JAVA_HOME=/usr/lib/jvm/wecogno-java

# Driver JDBC do PostgreSQL usado pelo Spark (spark.jars)
ARG PG_JDBC_VERSION=42.7.4
RUN mkdir -p /opt/spark-jars \
 && curl -fsSL -o /opt/spark-jars/postgresql-${PG_JDBC_VERSION}.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar
ENV PG_JDBC_JAR=/opt/spark-jars/postgresql-${PG_JDBC_VERSION}.jar

USER airflow

COPY requirements.txt /tmp/requirements.txt
# As constraints do Airflow garantem versões compatíveis (pyspark 3.5.4, pyarrow, numpy)
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
