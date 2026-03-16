from pyspark.sql import SparkSession
import os

def get_spark_session():
    jdbc_jar = os.path.abspath('drivers/mssql-jdbc-13.2.1.jre11.jar')

    spark = (SparkSession.builder
             .appName('BigFivePersonality')
             .master('local[*]')
             .config('spark.jars', jdbc_jar)
             .config('spark.driver.memory', '4g')
             .getOrCreate())

    spark.sparkContext.setLogLevel('WARN')
    return spark

if __name__ == '__main__':
    spark = get_spark_session()
    print(f"Spark version: {spark.version}")
    print('Session OK!')
    spark.stop()