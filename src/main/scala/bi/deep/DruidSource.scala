package bi.deep

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.jdk.CollectionConverters._


class DruidSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty, options).schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val config = Config(new CaseInsensitiveStringMap(properties), SparkContext.getOrCreate().hadoopConfiguration)
    new DruidTable(config, schema)
  }

  override def shortName(): String = "druid-segment"
}