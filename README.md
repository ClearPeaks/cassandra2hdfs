# Import external Cassandra table into HDFS

This script imports the content of a external Cassandra table into HDFS.

## Context

Our client has a Spark cluster and they wanted to migrate data from an external Cassandra cluster into this cluster. 
The only approach feasible we encountered was to develop this script, that ingests data from a specific Cassandra table into HDFS in ORC format.

## How to run it 

### Variables definition 

At the beginning of the script, there is a VARIABLE DEFINITION section where we placed all the variables that you will need to adapt. The rest of the code shouldn't need any adaptation.

```scala
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
```

While most of the variables are easy to understand and straight forward to modify, the first one which is the class MyCassandraTable is the schema of the table we want to ingest. Specifically, in this example, there are two columns called col1 and col2, which are of type String and Long respectively.

### Build and run commands

To run our script, simply build the project with the command

```sh
sbt package
```

And then submit the spark appliaction with the command:

```sh
spark-submit --class "SimpleApp" --packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.2,commons-configuration:commons-configuration:1.9 target/scala-2.11/simple-project_2.11-1.0.jar
```

Once finished, the table should be created into HDFS.

## Things to consider

As there is not native connectors in HDFS to connect to an external Cassandra database, we are using the [Spark Cassandra Connector from Datastax](https://github.com/datastax/spark-cassandra-connector).

You have to be aware of the versions you are using, in our case, we used:

- Spark 2.3.2
- Cassandra 3.11.2
- Java 8
- Scala 2.11

So we had to use the connector version 2.3.2.

Side note: In this cassandra connector version, we had to add the package `commons-configuration:commons-configuration:1.9` to avoid the error `java.lang.NoClassDefFoundError: org/apache/commons/configuration/ConfigurationException`.

## Contact us

Script developed by Victor Colome - victor.colome@clearpeaks.com\
Seeking for help? Contact us - info@clearpeaks.com
