from dvd_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, API_KEY, API_SECRET
from aws import AWS_ACCESS, AWS_SECRET
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pyspark.sql.functions as F


spark.conf.set("fs.s3a.access.key", AWS_ACCESS)
spark.conf.set("fs.s3a.secret.key", AWS_SECRET)
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")


def read_kafka(server, topic, api, secret):
    # read from topic
    dfread = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                api, secret
            ),
        )
        .load()
    )

    return dfread


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

    return transformed_df


def writetos3(df, path):
    """
    Start streaming the transformed data to the specified S3 bucket in parquet format.

    :param df: Transformed dataframe.
    :param path: S3 bucket path.
    :return: None
    """
    df.write.mode("append").parquet(path)


def main():
    df = read_kafka(
        server=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
        api=API_KEY,
        secret=API_SECRET,
    )
    if df:
        transformed_df = transform_kafka_json(df=df)
        writetos3(df=transformed_df, path="s3a://bucket_name")


# Execute the main function if this script is run as the main module
if __name__ == "__main__":
    main()
