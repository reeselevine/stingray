package ucsc.stingray

import ucsc.stingray.StingrayApp.{DataSchema, DataTypes, SetupConfig, TableSetup, TestColumn, WriteSkew}
import ucsc.stingray.StingrayDriver.DriverConfig

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {
    val schema = DataSchema("id", Map(
      "x" -> DataTypes.Integer,
      "y" -> DataTypes.Integer,
      "r0" -> DataTypes.Integer,
      "r1" -> DataTypes.Integer))
    val setupConfig = SetupConfig(Map("litmustest0" -> TableSetup(schema, 1), "litmustest1" -> TableSetup(schema, 1)))
    val test = WriteSkew(
      TestColumn("x", "r0", "litmustest0", 0),
      TestColumn("y", "r1", "litmustest1", 0),
      schema)
    val connectionPool = new JdbcConnectionPool()
    val sqlLikeClient = JdbcClient(connectionPool)
    //val cqlYugabyteClient = CqlYugabyteClient()
    val stingrayApp = SqlLikeStingrayApp(sqlLikeClient)
    val stingrayDriver = StingrayDriver(stingrayApp, DriverConfig(setupConfig, test))
    println("Starting test")
    Await.result(stingrayDriver.execute(), Duration.Inf)
    println("Test finished")
  }
}
