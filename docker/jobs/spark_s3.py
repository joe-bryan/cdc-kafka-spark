import logging
from pyspark.sql import SparkSession
from dvd_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from aws import AWS_ACCESS, AWS_SECRET
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as F


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("spark_batch_upload")


def create_spark_connection(access, secret):
    spark_connection = None

    try:
        spark_connection = (
            SparkSession.builder.appName("Spark_Kafka_Batch_Upload")
            .config("spark.hadoop.fs.s3a.access.key", access)
            .config("spark.hadoop.fs.s3a.secret.key", secret)
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.kafka:kafka-clients:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.2.0,"
                "com.amazonaws:aws-java-sdk-s3:1.12.600,"
                "org.apache.commons:commons-pool2:2.12.0",
            )
            .getOrCreate()
        )

        spark_connection.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_connection


def read_kafka(connection, server, topic):
    spark_dataframe = None
    try:
        # read from topic
        spark_dataframe = (
            connection.read.format("kafka")
            .option("kafka.bootstrap.servers", server)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
        ).load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_dataframe


def transform_kafka_json(df):
    schema = StructType(
        [
            StructField("rental_id", StringType(), False),
            StructField("rental_date", StringType(), False),
            StructField("inventory_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("return_date", StringType(), False),
            StructField("staff_id", StringType(), False),
            StructField("last_update", FloatType(), False),
        ]
    )

    transformed_df = df.withColumn(
        "value", F.from_json(F.col("value").cast("string"), schema)
    ).select(
        F.col("value.rental_id"),
        F.col("value.rental_date"),
        F.col("value.inventory_id"),
        F.col("value.customer_id"),
        F.col("value.return_date"),
        F.col("value.staff_id"),
        F.col("value.last_update"),
    )

    transformed_df = transformed_df.drop_duplicates()

    print(transformed_df)

    return transformed_df


def writetos3(df, path):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.

    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    logger.info("Initiating streaming process...")
    df.write.mode("append").parquet(path)


def main():
    # Create Spark connection
    spark = create_spark_connection(access=AWS_ACCESS, secret=AWS_SECRET)

    if spark:
        df = read_kafka(
            connection=spark, server=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC
        )
        if df:
            transformed_df = transform_kafka_json(df=df)
            writetos3(df=transformed_df, path="s3a://bucket-name")


# Execute the main function if this script is run as the main module
if __name__ == "__main__":
    main()
