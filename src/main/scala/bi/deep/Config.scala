package bi.deep

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.time.LocalDate
import scala.jdk.CollectionConverters._


case class Config
(
  dataSource: String,
  startDate: LocalDate,
  endDate: LocalDate,
  factory: FileSystemFactory,
  druidTimestamp: String = Config.DEFAULT_DRUID_TIMESTAMP,
  inputPath: String,
  tempSegmentDir: String
)

object Config {

  val DEFAULT_DRUID_TIMESTAMP = ""
  val DEFAULT_TEMP_SEGMENT_DIR = ""
  val DRUID_SEGMENT_FILE = "index.zip"

  def getRequired(optionsMap: Map[String, String], fieldName: String): String = optionsMap.get(fieldName) match {
    case None => throw new RuntimeException(s"Field $fieldName is required, but was missing.")
    case Some(x) => x
  }

  def apply(options: CaseInsensitiveStringMap, hadoopConfig: Configuration): Config = {
    val optionsMap = options.asScala.toMap
    new Config(
      dataSource = getRequired(optionsMap, "druid_data_source"),
      startDate = LocalDate.parse(getRequired(optionsMap, "start_date")),
      endDate = LocalDate.parse(getRequired(optionsMap, "end_date")),
      factory = new FileSystemFactory(new SerializableHadoopConfiguration(hadoopConfig)),
      druidTimestamp = optionsMap.getOrElse("druid_timestamp", DEFAULT_DRUID_TIMESTAMP),
      inputPath = getRequired(optionsMap, "input_path"),
      tempSegmentDir = optionsMap.getOrElse("temp_segment_dir", DEFAULT_TEMP_SEGMENT_DIR)
    )
  }
}


// Spark 3.x has built-in SerializableConfiguration, but we keep a wrapper for compatibility
@SerialVersionUID(1L)
class SerializableHadoopConfiguration(@transient var value: Configuration) extends Serializable {
  
  private def writeObject(out: java.io.ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: java.io.ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }

  def config: Configuration = value
}