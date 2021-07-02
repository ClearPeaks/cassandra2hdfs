import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

object SimpleApp extends Serializable {

  /**************************************************************************************
    * VARIABLES DEFINITION
    * Adapt the variables below
    *************************************************************************************/

  // Representative of the my_keyspace.my_table table.
  case class MyCassandraTable(col1: String, col2: Long)
  // Cassandra keyspace
  val keyspace = "my_keyspace"
  // Name of the table to ingest from Cassandra
  val table_name = "my_table"
  // Name of the HDFS table - default is same name
  val output_table_name = table_name
  // Cassandra user and password
  val user = Map("spark.cassandra.auth.username" -> "my_user")
  val pwd = Map("spark.cassandra.auth.password" -> "my_password")
  // Cassandra cluster connection info
  val hosts = "000.00.0.0"
  val port = "9042" // default port


  /**************************************************************************************
    * MAIN CODE
    * The code below shouldn't be changed - unless you know what you are doing
    *************************************************************************************/
  def main(args: Array[String]) { // scalastyle:off method.length

    /**************************************************************************************
      * INITIATE SPARK SESSION
      * This session will be used to ingest data from a Cassandra cluster.
      *************************************************************************************/
    val spark = SparkSession
      .builder()
      .config("hive.merge.orcfile.stripe.level", "false")
      .appName("Cassandra Data Loader")
      .enableHiveSupport()
      .getOrCreate()

    // Implicits allow us to use .as[CaseClass]
    import spark.implicits._
    // Encoders allow us to use .schema(schema)
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[MyCassandraTable].schema
    
    /**************************************************************************************
      * SETUP CASSANDRA CONNECTION
      * These settings determine which environment, keyspace and table to download.
      *************************************************************************************/

    // Set Cassandra Connection Configuration in Spark Session
    spark.setCassandraConf(
      CassandraConnectorConf.KeepAliveMillisParam.option(1000) ++
        CassandraConnectorConf.ConnectionHostParam.option(hosts) ++
        CassandraConnectorConf.ReadTimeoutParam.option(240000) ++
        CassandraConnectorConf.ConnectionPortParam.option(port) ++
        user ++ pwd)

    // Imply which keyspace.table to consume from Cassandra
    val table = Map("keyspace" -> keyspace, "table" -> table_name)

    /**************************************************************************************
      * CONSUME DATA FROM CASSANDRA
      * Use the connector, via the format() method, to pull the data and write it.
      *************************************************************************************/
    val data = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .schema(schema)
      .options(table)
      .load()
      .as[MyCassandraTable]

    // write to hdfs
    data
      .write
      .option("orc.compress", "snappy")
      .mode(SaveMode.Overwrite)
      .orc(output_table_name)
  }
}
