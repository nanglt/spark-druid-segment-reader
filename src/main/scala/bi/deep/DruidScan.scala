package bi.deep

import bi.deep.segments.SegmentStorage
import bi.deep.utils.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.types.StructType


class DruidScan(config: Config, userSchema: Option[StructType]) extends Scan with Logging {

  // Lazily infer schema from segments if not provided by user
  @transient
  private lazy val inferredSchema: StructType = {
    logInfo(s"Inferring schema from Druid segments for datasource ${config.dataSource}")
    
    val mainPath = new Path(config.inputPath, config.dataSource)
    implicit val fs: FileSystem = config.factory.fileSystemFor(mainPath)
    val storage = SegmentStorage(mainPath)
    val schemaReader = DruidSchemaReader(config)
    val filesPaths = storage.findValidSegments(config.startDate, config.endDate)
    
    val schema = schemaReader.calculateSparkSchema(filesPaths).mergedSchema
    logInfo(s"Inferred schema with ${schema.fields.length} columns")
    schema
  }

  // The actual schema to use: user-provided or inferred
  @transient
  private lazy val actualSchema: StructType = userSchema.getOrElse(inferredSchema)

  override def readSchema(): StructType = actualSchema

  override def toBatch: Batch = {
    new DruidBatch(config, actualSchema)
  }

  override def description(): String = {
    s"DruidScan[${config.dataSource}, ${config.startDate} to ${config.endDate}]"
  }
}

