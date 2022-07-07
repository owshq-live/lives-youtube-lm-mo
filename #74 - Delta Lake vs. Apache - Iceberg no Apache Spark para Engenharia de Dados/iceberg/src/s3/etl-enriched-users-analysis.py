# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import current_timestamp, col

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
        .config("fs.s3a.connection.maximum", 100) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.owshq", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.owshq.type", "hadoop") \
        .config("spark.sql.catalog.owshq.s3.endpoint", "http://minio-ip") \
        .config("spark.sql.catalog.owshq.warehouse", "s3a://lakehouse/iceberg/") \
        .getOrCreate()

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")

    # reading files at scale
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

    # create database first
    create_db = spark.sql("""CREATE DATABASE IF NOT EXISTS metastore;""")
    create_db.show()

    # create iceberg table
    create_device_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.metastore.device
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
        CREATE TABLE IF NOT EXISTS owshq.metastore.subscription
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
    df_device.createOrReplaceTempView("vw_device")
    df_subscription.createOrReplaceTempView("vw_subscription")

    # insert into table using new v2 dataframe write api
    spark.table("vw_device").writeTo("owshq.metastore.device").append()
    spark.table("vw_subscription").writeTo("owshq.metastore.subscription").append()

    # select data & query data
    query_device = spark.table("owshq.metastore.device")
    query_subscription = spark.table("owshq.metastore.subscription")

    # amount of records written
    # 400
    print(query_device.count())
    print(query_subscription.count())

    # applying enrichment
    # select columns from dataframes to reduce footprint
    select_columns_device = df_device.select("user_id", "uid", "model", "manufacturer", "platform", "dt_current_timestamp")
    select_columns_subscription = df_subscription.select("user_id", "plan", "status", "dt_current_timestamp")

    # select columns to be used
    # same data coming from data lake
    enhance_column_selection_device = select_columns_device.select(
        col("user_id").alias("device_user_id"),
        col("model").alias("device_model"),
        col("dt_current_timestamp").alias("device_event_time"),
    ).distinct()

    # make it available into spark's sql engine
    enhance_column_selection_device.createOrReplaceTempView("vw_device")

    # [udf] in python
    # business transformations
    def subscription_importance(subscription_plan):
        if subscription_plan in ("Business", "Diamond", "Gold", "Platinum", "Premium"):
            return "High"
        if subscription_plan in ("Bronze", "Essential", "Professional", "Silver", "Standard"):
            return "Normal"
        else:
            return "Low"

    # register function into spark's engine to make it available
    # once registered you can access in any language
    spark.udf.register("fn_subscription_importance", subscription_importance)

    # select columns of subscription
    # use alias to save your upfront process ~ silver
    # better name understanding for the business
    enhance_column_selection_subscription = select_columns_subscription.select(
        col("user_id").alias("subscription_user_id"),
        col("plan").alias("subscription_plan"),
        col("status").alias("subscription_status"),
        col("dt_current_timestamp").alias("subscription_event_time"),
    ).distinct()

    # register as a spark sql object
    enhance_column_selection_subscription.createOrReplaceTempView("vw_subscription")

    # build another way to create functions
    # using spark sql engine capability to perform a case when
    # save the sql into a dataframe
    enhance_column_selection_subscription = spark.sql("""
    SELECT subscription_user_id,
           subscription_plan,
           CASE WHEN subscription_plan = 'Basic' THEN 6.00
                WHEN subscription_plan = 'Bronze' THEN 8.00
                WHEN subscription_plan = 'Business' THEN 10.00
                WHEN subscription_plan = 'Diamond' THEN 14.00
                WHEN subscription_plan = 'Essential' THEN 9.00
                WHEN subscription_plan = 'Free Trial' THEN 0.00
                WHEN subscription_plan = 'Gold' THEN 25.00
                WHEN subscription_plan = 'Platinum' THEN 9.00
                WHEN subscription_plan = 'Premium' THEN 13.00
                WHEN subscription_plan = 'Professional' THEN 17.00
                WHEN subscription_plan = 'Silver' THEN 11.00
                WHEN subscription_plan = 'Standard' THEN 13.00
                WHEN subscription_plan = 'Starter' THEN 5.00
                WHEN subscription_plan = 'Student' THEN 2.00
           ELSE 0.00 END AS subscription_price,
           subscription_status,
           fn_subscription_importance(subscription_plan) AS subscription_importance,
           subscription_event_time AS subscription_event_time
    FROM vw_subscription
    """)

    # show & count df
    enhance_column_selection_device.explain()
    enhance_column_selection_device.count()
    enhance_column_selection_subscription.explain()
    enhance_column_selection_subscription.count()

    # show df
    enhance_column_selection_device.show()
    enhance_column_selection_subscription.show()

    # perform inner join between the subscription and device
    # figure out which devices are being used to watch movies
    inner_join_subscriptions = enhance_column_selection_subscription.join(
        enhance_column_selection_device,
        enhance_column_selection_subscription.subscription_user_id == enhance_column_selection_device.device_user_id,
        how='inner'
    )

    # show join
    # grouped result set
    inner_join_subscriptions.show()

    # select columns for analysis
    # the latest version
    select_columns_subscriptions = inner_join_subscriptions.select("subscription_plan", "subscription_price", "subscription_importance", "device_model")
    select_columns_subscriptions.show()

    # add timestamp column
    # generated column into df
    get_plans_df = select_columns_subscriptions.withColumn("subscription_event_time", current_timestamp())
    get_correct_columns = get_plans_df.select(
        col("subscription_plan").alias("plan"),
        col("subscription_price").alias("price"),
        col("subscription_importance").alias("importance"),
        col("device_model").alias("model"),
        col("subscription_event_time").alias("event_time")
    )

    # create new iceberg table
    create_plans_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.metastore.plans
        (
            plan string,
            price decimal(4,2),
            importance string,
            model string,
            event_time timestamp
        )
        USING iceberg
    """)

    # insert into table using new v2 dataframe write api
    get_correct_columns.createOrReplaceTempView("vw_plans")
    spark.table("vw_plans").writeTo("owshq.metastore.plans").append()

    # stop session
    spark.stop()
