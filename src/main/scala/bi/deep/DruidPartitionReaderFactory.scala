package bi.deep

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType


case class DruidPartitionReaderFactory(schema: StructType, config: Config) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val druidPartition = partition.asInstanceOf[DruidInputPartition]
    DruidPartitionReader(druidPartition.filePath, druidPartition.schema, druidPartition.config)
  }
}

