package bi.deep

import org.apache.spark.sql.connector.read.{Batch, Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType


case class DruidScanBuilder(config: Config, userSchema: Option[StructType]) extends ScanBuilder {

  override def build(): Scan = {
    new DruidScan(config, userSchema)
  }
}

