package bi.deep

import bi.deep.segments.{Segment, SegmentStorage}
import bi.deep.utils.Logging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._


class DruidBatch(config: Config, providedSchema: StructType) extends Batch with Logging {

  @transient
  private lazy val mainPath: Path = new Path(config.inputPath, config.dataSource)

  @transient
  private implicit lazy val fs: FileSystem = config.factory.fileSystemFor(mainPath)

  @transient
  private lazy val storage = SegmentStorage(mainPath)

  @transient
  private lazy val schemaReader = DruidSchemaReader(config)

  @transient
  private lazy val filesPaths: Seq[Segment] = storage.findValidSegments(config.startDate, config.endDate)

  @transient
  lazy val schema: StructType = {
    if (providedSchema != null) providedSchema
    else schemaReader.calculateSparkSchema(filesPaths).mergedSchema
  }

  private def fileToInputPartition(file: Segment): InputPartition = {
    DruidInputPartition(file.path.toString, schema, config)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    val partitions = filesPaths.map(fileToInputPartition).toArray
    logInfo(s"Planning ${partitions.length} input partitions for datasource ${config.dataSource} " +
      s"from ${config.startDate} to ${config.endDate}")
    partitions
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    DruidPartitionReaderFactory(schema, config)
  }
}
