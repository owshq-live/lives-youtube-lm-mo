# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
        .builder \
        .appName("etl-enriched-users-analysis") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-ip") \
        .config("spark.hadoop.fs.s3a.access.key", "YourAccessKey") \
        .config("spark.hadoop.fs.s3a.secret.key", "YourSecretKey") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.fast.upload", True) \
        .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # get_device_file = "s3a://landing/device/*.json"
    # get_subscription_file = "s3a://landing/subscription/*.json"
    get_device_file = "s3a://landing/device/device_2022_6_10_0_0_31.json"
    get_subscription_file = "s3a://landing/subscription/subscription_2022_6_10_0_0_28.json"

    # read device data
    # json file from landing zone
    df_device = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json(get_device_file)

    # read subscription data
    # json file from landing zone
    df_subscription = spark.read \
        .format("json") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json(get_subscription_file)

    # get number of partitions
    print(df_device.rdd.getNumPartitions())
    print(df_subscription.rdd.getNumPartitions())

    # count amount of rows ingested from lake
    df_device.count()
    df_subscription.count()

    # show datasets
    df_device.show()
    df_subscription.show()

    # create iceberg table
    create_device_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.device
        (
            build_number int,
            dt_current_timestamp long,
            id int,
            manufacturer string,
            model string,
            platform string,
            serial_number string,
            uid string,
            user_id int,
            version int
        )
        USING iceberg
    """)

    create_subscription_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.subscription
        (
            dt_current_timestamp long,
            id int,
            payment_method string,
            payment_term string,
            plan string,
            status string,
            subscription_term string,
            uid string,
            user_id int
        )
        USING iceberg
    """)

    # reference to spark sql engine
    df_device.createOrReplaceTempView("device")
    df_subscription.createOrReplaceTempView("subscription")

    # insert into table using new v2 dataframe write api
    spark.table("device").writeTo("local.db.device").append()
    spark.table("subscription").writeTo("local.db.subscription").append()

    # select data & query data
    # query_device = spark.sql("""SELECT COUNT(*) FROM local.db.device""")
    # query_subscription = spark.sql("""SELECT COUNT(*) FROM local.db.subscription""")
    query_device = spark.table("local.db.device")
    query_subscription = spark.table("local.db.device")

    # amount of records written
    print(query_device.count())
    print(query_subscription.count())

    # stop session
    spark.stop()
