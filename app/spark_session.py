from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config import Config

# if __name__ == "__main__":

scala_version = '3.2.2'
spark_version = '3.3.2'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.4.0'
]
spark = (SparkSession.builder
         .appName("KafkaPysparkStreaming")
         .master("local[*]")
         .config("spark.jars.packages", ",".join(packages))
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")
config = Config()
sampleDataFrame = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", config.broker)
    .option("subscribe", config.topic)
    .option("startingOffsets", "latest")
    .load()
)
base_df = sampleDataFrame.selectExpr("CAST(value as STRING)", "timestamp")
base_df.printSchema()

query = sampleDataFrame \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
