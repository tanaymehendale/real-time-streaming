from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_schema_with_connector(spark):
    jvm = spark._jvm
    conf = spark._jsparkSession.sparkContext().getConf()
    connector = jvm.com.datastax.spark.connector.cql.CassandraConnector.apply(conf)
    session = connector.openSession()
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class':'SimpleStrategy','replication_factor':'1'};
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                post_code TEXT,
                email TEXT,
                username TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT
            );
        """)
    finally:
        session.close()

spark = (SparkSession.builder
    .appName("KafkaToCassandra")
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Ensure keyspace/table exist
create_schema_with_connector(spark)

# Read from Kafka (inside Docker network)
df = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")
    .option("kafka.allow.auto.create.topics", "true")
    .option("subscribe", "users_created")
    .option("startingOffsets", "earliest")
    .load())

schema = StructType([
    StructField("id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("address", StringType(), False),
    StructField("post_code", StringType(), False),
    StructField("email", StringType(), False),
    StructField("username", StringType(), False),
    StructField("dob", StringType(), False),
    StructField("registered_date", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("picture", StringType(), False),
])

parsed = (df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*"))

(parsed.writeStream
    .format("org.apache.spark.sql.cassandra")
    .option("checkpointLocation", "/tmp/checkpoints/users_created")
    .option("keyspace", "spark_streams")
    .option("table", "created_users")
    .start()
    .awaitTermination())