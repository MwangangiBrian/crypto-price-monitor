import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS crypto_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )
    logging.info("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_streams.created_cryptos (
            id UUID,
            symbol TEXT,
            name TEXT,
            rank INT,
            price FLOAT,
            price_change_24h FLOAT,
            volume FLOAT,
            volume_24h FLOAT,
            volume_change_24h FLOAT,
            market_cap FLOAT,
            updated_at TIMESTAMP,
            PRIMARY KEY ((symbol), updated_at)
        );
        """
    )
    logging.info("Table created successfully!")


def create_spark_connection():
    try:
        spark_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "cryptos_created")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created because: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("name", StringType(), True),
            StructField("rank", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("price_change_24h", FloatType(), True),
            StructField("volume", FloatType(), True),
            StructField("volume_24h", FloatType(), True),
            StructField("volume_change_24h", FloatType(), True),
            StructField("market_cap", FloatType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )

    sel_df = (
        spark_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    logging.info("Selection DataFrame created successfully!")
    return sel_df


if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            logging.info("Streaming is being started...")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "crypto_streams")
                .option("table", "created_cryptos")
                .start()
            )

            streaming_query.awaitTermination()
