from pyspark.sql import SparkSession
import os
import logging


logger = logging.getLogger(__name__)


def get_spark_session(
    jdbc_jar: str = None,
    app_name: str = None,
    driver_memory: str = None,
):
    """
    Create a SparkSession. Parameters default to environment variables or safe fallbacks.
    - JDBC_JAR: path to JDBC driver jar (env JDBC_JAR)
    - SPARK_APP_NAME: app name (env SPARK_APP_NAME)
    - SPARK_DRIVER_MEMORY: driver memory (env SPARK_DRIVER_MEMORY)
    """
    jdbc_jar = (
        jdbc_jar
        or os.getenv("JDBC_JAR")
        or os.path.abspath("drivers/mssql-jdbc-13.2.1.jre11.jar")
    )
    app_name = app_name or os.getenv("SPARK_APP_NAME", "BigFivePersonality")
    driver_memory = driver_memory or os.getenv("SPARK_DRIVER_MEMORY", "4g")

    builder = SparkSession.builder.appName(app_name).master(
        os.getenv("SPARK_MASTER", "local[*]")
    )

    if jdbc_jar and os.path.exists(jdbc_jar):
        builder = builder.config("spark.jars", os.path.abspath(jdbc_jar))
    else:
        # If no jar present, continue but warn (caller should ensure driver availability).
        logger.warning(
            "JDBC jar not found at %s (continuing without spark.jars)", jdbc_jar
        )

    builder = builder.config("spark.driver.memory", driver_memory)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


if __name__ == "__main__":
    s = get_spark_session()
    print(f"Spark version: {s.version}")
    print("Session OK!")
    s.stop()
