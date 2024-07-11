# Use the official Apache Airflow image
FROM apache/airflow:2.1.2

# Install additional dependencies
RUN pip install apache-airflow-providers-apache-spark \
                apache-airflow-providers-postgres \
                pyspark

# Set environment variables for Spark
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Set the working directory
WORKDIR /opt/airflow

# Copy the DAGs
COPY dags /opt/airflow/dags

# Copy the PySpark scripts
COPY scripts /opt/airflow/scripts
