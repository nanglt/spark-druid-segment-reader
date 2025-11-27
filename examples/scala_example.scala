package examples

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Example: Reading Druid Segments with Scala
 *
 * This example demonstrates how to use the Spark Druid Segment Reader
 * to load data directly from Druid segments stored in S3 or HDFS.
 *
 * Requirements:
 * - Apache Spark 3.5.6
 * - Scala 2.12.18
 * - spark-druid-segment-reader JAR in classpath
 */
object DruidSegmentReaderExample {

  /**
   * Create a SparkSession configured for reading Druid segments.
   */
  def createSparkSession(appName: String = "Druid Segment Reader Example"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.executor.memory", "4g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()
  }

  /**
   * Configure S3 access credentials for the Spark session.
   */
  def configureS3Access(spark: SparkSession, endpoint: String, accessKey: String, secretKey: String): Unit = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.s3a.endpoint", endpoint)
    hadoopConf.set("fs.s3a.access.key", accessKey)
    hadoopConf.set("fs.s3a.secret.key", secretKey)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  }

  /**
   * Read Druid segments from S3.
   */
  def readDruidSegmentsFromS3(
      spark: SparkSession,
      datasource: String,
      startDate: String,
      endDate: String,
      s3Path: String,
      timestampColumn: Option[String] = None
  ): DataFrame = {
    
    println(s"Reading Druid segments from $s3Path$datasource")
    println(s"Date range: $startDate to $endDate")

    val reader = spark.read
      .format("druid-segment")
      .option("druid_data_source", datasource)
      .option("start_date", startDate)
      .option("end_date", endDate)
      .option("input_path", s3Path)

    val df = timestampColumn match {
      case Some(col) => reader.option("druid_timestamp", col).load()
      case None => reader.load()
    }

    println(s"Loaded DataFrame with ${df.count()} rows")
    df
  }

  /**
   * Read Druid segments from HDFS.
   */
  def readDruidSegmentsFromHDFS(
      spark: SparkSession,
      datasource: String,
      startDate: String,
      endDate: String,
      hdfsPath: String = "hdfs://namenode:9000/druid/segments/",
      timestampColumn: Option[String] = None
  ): DataFrame = {
    
    println(s"Reading Druid segments from $hdfsPath$datasource")
    println(s"Date range: $startDate to $endDate")

    val reader = spark.read
      .format("druid-segment")
      .option("druid_data_source", datasource)
      .option("start_date", startDate)
      .option("end_date", endDate)
      .option("input_path", hdfsPath)

    val df = timestampColumn match {
      case Some(col) => reader.option("druid_timestamp", col).load()
      case None => reader.load()
    }

    println(s"Loaded DataFrame with ${df.count()} rows")
    df
  }

  /**
   * Basic usage example.
   */
  def basicUsageExample(): Unit = {
    val spark = createSparkSession()

    try {
      // Configure S3 access
      configureS3Access(
        spark,
        endpoint = "https://s3.amazonaws.com",
        accessKey = "YOUR_ACCESS_KEY",
        secretKey = "YOUR_SECRET_KEY"
      )

      // Read Druid segments
      val df = readDruidSegmentsFromS3(
        spark,
        datasource = "my_datasource",
        startDate = "2024-01-01",
        endDate = "2024-01-31",
        s3Path = "s3a://my-bucket/druid/segments/",
        timestampColumn = Some("event_timestamp")
      )

      // Display schema
      println("Schema:")
      df.printSchema()

      // Show sample data
      println("\nSample data:")
      df.show(10)

      // Perform analytics
      println("\nData summary:")
      df.describe().show()

      // Filter and aggregate
      val filteredDf = df.filter(df("event_timestamp") > "2024-01-15")
      println(s"\nFiltered count: ${filteredDf.count()}")

    } finally {
      spark.stop()
    }
  }

  /**
   * Advanced usage example with Spark SQL.
   */
  def advancedUsageExample(): Unit = {
    val spark = createSparkSession("Advanced Druid Example")

    try {
      // Read from HDFS
      val df = readDruidSegmentsFromHDFS(
        spark,
        datasource = "events",
        startDate = "2024-11-01",
        endDate = "2024-11-27",
        hdfsPath = "hdfs://namenode:9000/druid/segments/"
      )

      // Register as temporary view
      df.createOrReplaceTempView("druid_events")

      // Run SQL queries
      val result = spark.sql(
        """
          |SELECT
          |  DATE(event_timestamp) as date,
          |  COUNT(*) as event_count,
          |  COUNT(DISTINCT user_id) as unique_users
          |FROM druid_events
          |GROUP BY DATE(event_timestamp)
          |ORDER BY date
        """.stripMargin
      )

      result.show()

      // Save results to Parquet
      result.write.mode("overwrite").parquet("/output/aggregated_events")
      println("Results saved to /output/aggregated_events")

    } finally {
      spark.stop()
    }
  }

  /**
   * Example with type-safe Dataset API.
   */
  def typeSafeExample(): Unit = {
    val spark = createSparkSession("Type-Safe Druid Example")
    import spark.implicits._

    // Define case class for your data
    case class Event(
      timestamp: Long,
      userId: String,
      eventType: String,
      value: Double
    )

    try {
      val df = readDruidSegmentsFromS3(
        spark,
        datasource = "events",
        startDate = "2024-11-01",
        endDate = "2024-11-27",
        s3Path = "s3a://my-bucket/druid/segments/"
      )

      // Convert to Dataset
      val events: org.apache.spark.sql.Dataset[Event] = df.as[Event]

      // Type-safe transformations
      val userStats = events
        .groupByKey(_.userId)
        .mapGroups { case (userId, events) =>
          val eventList = events.toList
          (userId, eventList.size, eventList.map(_.value).sum)
        }
        .toDF("user_id", "event_count", "total_value")

      userStats.show()

    } finally {
      spark.stop()
    }
  }

  /**
   * Example of incremental processing.
   */
  def incrementalProcessingExample(): Unit = {
    val spark = createSparkSession("Incremental Processing")

    try {
      import java.time.LocalDate
      import java.time.format.DateTimeFormatter

      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val start = LocalDate.parse("2024-11-01", formatter)
      val end = LocalDate.parse("2024-11-27", formatter)

      var current = start
      while (!current.isAfter(end)) {
        val dateStr = current.format(formatter)
        println(s"\nProcessing $dateStr...")

        val df = readDruidSegmentsFromS3(
          spark,
          datasource = "daily_events",
          startDate = dateStr,
          endDate = dateStr,
          s3Path = "s3a://my-bucket/druid/segments/"
        )

        // Process this day's data
        val dailyCount = df.count()
        println(s"Processed $dailyCount records for $dateStr")

        // Save daily results
        df.write.mode("overwrite").parquet(s"/output/daily/$dateStr")

        current = current.plusDays(1)
      }

      println("\nIncremental processing completed!")

    } finally {
      spark.stop()
    }
  }

  /**
   * Example with custom temp directory for segments.
   */
  def customTempDirExample(): Unit = {
    val spark = createSparkSession()

    try {
      val df = spark.read
        .format("druid-segment")
        .option("druid_data_source", "my_datasource")
        .option("start_date", "2024-11-01")
        .option("end_date", "2024-11-27")
        .option("input_path", "s3a://my-bucket/druid/segments/")
        .option("temp_segment_dir", "/tmp/custom_segments/")  // Custom temp directory
        .load()

      df.show()

    } finally {
      spark.stop()
    }
  }

  /**
   * Main entry point - run all examples.
   */
  def main(args: Array[String]): Unit = {
    println("Spark Druid Segment Reader Examples")
    println("====================================\n")

    // Uncomment the example you want to run:

    // basicUsageExample()
    // advancedUsageExample()
    // typeSafeExample()
    // incrementalProcessingExample()
    // customTempDirExample()

    println("Please uncomment one of the example functions to run.")
  }
}

