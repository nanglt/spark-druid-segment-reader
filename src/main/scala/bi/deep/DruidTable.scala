package bi.deep

import bi.deep.segments.SegmentStorage
import bi.deep.utils.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._


class DruidTable(config: Config, userSchema: StructType) extends Table with SupportsRead with Logging {

  override def name(): String = s"druid_segment[${config.dataSource}]"

  // Lazily compute and cache the schema
  @transient
  private lazy val tableSchema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      // Infer schema from segments
      logInfo(s"Inferring schema for table ${config.dataSource}")
      
      val mainPath = new Path(config.inputPath, config.dataSource)
      implicit val fs: FileSystem = config.factory.fileSystemFor(mainPath)
      val storage = SegmentStorage(mainPath)
      val schemaReader = DruidSchemaReader(config)
      val filesPaths = storage.findValidSegments(config.startDate, config.endDate)
      
      val schema = schemaReader.calculateSparkSchema(filesPaths).mergedSchema
      logInfo(s"Inferred schema with ${schema.fields.length} columns")
      schema
    }
  }

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // Pass the already-computed schema to the scan builder
    DruidScanBuilder(config, Some(tableSchema))
  }
}

