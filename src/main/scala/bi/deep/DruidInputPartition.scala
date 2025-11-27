package bi.deep

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType


case class DruidInputPartition(filePath: String, schema: StructType, config: Config) extends InputPartition {
  
  override def preferredLocations(): Array[String] = {
    // Could potentially implement data locality here
    Array.empty[String]
  }
}

