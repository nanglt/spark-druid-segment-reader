"""
Example: Reading Druid Segments with PySpark

This example demonstrates how to use the Spark Druid Segment Reader
to load data directly from Druid segments stored in S3 or HDFS.

Requirements:
- Apache Spark 3.5.6
- Python 3.8+
- spark-druid-segment-reader JAR file
"""

from pyspark.sql import SparkSession


def create_spark_session(jar_path):
    """
    Create a SparkSession with the Druid connector JAR.
    
    Args:
        jar_path: Path to the spark-druid-segment-reader assembly JAR
    
    Returns:
        SparkSession configured for reading Druid segments
    """
    return (
        SparkSession.builder
        .appName("Druid Segment Reader Example")
        .config("spark.jars", jar_path)
        # Adjust these based on your cluster resources
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def configure_s3_access(spark, endpoint, access_key, secret_key):
    """
    Configure S3 access credentials for the Spark session.
    
    Args:
        spark: SparkSession instance
        endpoint: S3 endpoint URL
        access_key: AWS access key
        secret_key: AWS secret key
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def read_druid_segments_s3(spark, datasource, start_date, end_date, 
                           s3_path, timestamp_column=None):
    """
    Read Druid segments from S3.
    
    Args:
        spark: SparkSession instance
        datasource: Name of the Druid datasource directory
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        s3_path: S3 path to Druid segments (e.g., "s3a://bucket/druid/segments/")
        timestamp_column: Optional name for the Druid timestamp column
    
    Returns:
        DataFrame containing the Druid data
    """
    properties = {
        "druid_data_source": datasource,
        "start_date": start_date,
        "end_date": end_date,
        "input_path": s3_path,
    }
    
    # Add timestamp column if specified
    if timestamp_column:
        properties["druid_timestamp"] = timestamp_column
    
    print(f"Reading Druid segments from {s3_path}{datasource}")
    print(f"Date range: {start_date} to {end_date}")
    
    df = spark.read.format("druid-segment").options(**properties).load()
    
    print(f"Loaded DataFrame with {df.count()} rows")
    return df


def read_druid_segments_hdfs(spark, datasource, start_date, end_date,
                             hdfs_path="/druid/segments/", timestamp_column=None):
    """
    Read Druid segments from HDFS.
    
    Args:
        spark: SparkSession instance
        datasource: Name of the Druid datasource directory
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        hdfs_path: HDFS path to Druid segments
        timestamp_column: Optional name for the Druid timestamp column
    
    Returns:
        DataFrame containing the Druid data
    """
    properties = {
        "druid_data_source": datasource,
        "start_date": start_date,
        "end_date": end_date,
        "input_path": hdfs_path,
    }
    
    if timestamp_column:
        properties["druid_timestamp"] = timestamp_column
    
    print(f"Reading Druid segments from {hdfs_path}{datasource}")
    print(f"Date range: {start_date} to {end_date}")
    
    df = spark.read.format("druid-segment").options(**properties).load()
    
    print(f"Loaded DataFrame with {df.count()} rows")
    return df


def example_basic_usage():
    """Basic example of reading Druid segments."""
    
    # 1. Create Spark session with the connector JAR
    jar_path = "/path/to/spark-druid-segment-reader-assembly-1.0.0.jar"
    spark = create_spark_session(jar_path)
    
    try:
        # 2. Configure S3 access (if reading from S3)
        configure_s3_access(
            spark,
            endpoint="https://s3.amazonaws.com",
            access_key="YOUR_ACCESS_KEY",
            secret_key="YOUR_SECRET_KEY"
        )
        
        # 3. Read Druid segments
        df = read_druid_segments_s3(
            spark,
            datasource="my_datasource",
            start_date="2024-01-01",
            end_date="2024-01-31",
            s3_path="s3a://my-bucket/druid/segments/",
            timestamp_column="event_timestamp"
        )
        
        # 4. Use the DataFrame
        print("Schema:")
        df.printSchema()
        
        print("\nSample data:")
        df.show(10)
        
        # 5. Perform analytics
        print("\nData summary:")
        df.describe().show()
        
        # 6. Filter and aggregate
        filtered_df = df.filter(df.event_timestamp > "2024-01-15")
        print(f"\nFiltered count: {filtered_df.count()}")
        
    finally:
        spark.stop()


def example_advanced_usage():
    """Advanced example with custom processing."""
    
    jar_path = "/path/to/spark-druid-segment-reader-assembly-1.0.0.jar"
    spark = create_spark_session(jar_path)
    
    try:
        # Read from HDFS
        df = read_druid_segments_hdfs(
            spark,
            datasource="events",
            start_date="2024-11-01",
            end_date="2024-11-27",
            hdfs_path="hdfs://namenode:9000/druid/segments/"
        )
        
        # Register as temporary view for SQL queries
        df.createOrReplaceTempView("druid_events")
        
        # Run SQL queries
        result = spark.sql("""
            SELECT 
                DATE(event_timestamp) as date,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM druid_events
            GROUP BY DATE(event_timestamp)
            ORDER BY date
        """)
        
        result.show()
        
        # Save results to Parquet
        result.write.mode("overwrite").parquet("/output/aggregated_events")
        
        print("Results saved to /output/aggregated_events")
        
    finally:
        spark.stop()


def example_incremental_processing():
    """Example of processing data incrementally by date."""
    
    jar_path = "/path/to/spark-druid-segment-reader-assembly-1.0.0.jar"
    spark = create_spark_session(jar_path)
    
    try:
        from datetime import datetime, timedelta
        
        # Process one day at a time
        start = datetime(2024, 11, 1)
        end = datetime(2024, 11, 27)
        
        current = start
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            
            print(f"\nProcessing {date_str}...")
            
            df = read_druid_segments_s3(
                spark,
                datasource="daily_events",
                start_date=date_str,
                end_date=date_str,
                s3_path="s3a://my-bucket/druid/segments/"
            )
            
            # Process this day's data
            daily_count = df.count()
            print(f"Processed {daily_count} records for {date_str}")
            
            # Save daily results
            df.write.mode("overwrite").parquet(f"/output/daily/{date_str}")
            
            current += timedelta(days=1)
        
        print("\nIncremental processing completed!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    # Run the basic example
    # Uncomment the example you want to run:
    
    # example_basic_usage()
    # example_advanced_usage()
    # example_incremental_processing()
    
    print("Please uncomment one of the example functions to run.")

