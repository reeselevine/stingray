package ucsc.stingray

import com.typesafe.config.ConfigFactory
import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  val schema = DataSchema("id", Map(
    "x" -> DataTypes.Integer,
    "y" -> DataTypes.Integer,
    "r0" -> DataTypes.Integer,
    "r1" -> DataTypes.Integer))
  val setupConfig = SetupConfig(Map("litmustest0" -> TableSetup(schema, 2), "litmustest1" -> TableSetup(schema, 2)))

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val isolationLevel = IsolationLevels.withName(config.getString("stingray.isolationLevel"))
    val test = config.getString("stingray.test") match {
      case "write-skew" => buildWriteSkewTest(schema, isolationLevel)
      case "dirty-write" => buildDirtyWriteTest(schema, isolationLevel)
    }
    val client = config.getString("stingray.client") match {
      case "SQL" =>
        val connectionPool = new JdbcConnectionPool()
        JdbcClient(connectionPool)
      case "CQL" => CqlYugabyteClient()
    }
    val stingrayApp = SqlLikeStingrayApp(client)
    val stingrayDriver = StingrayDriver(stingrayApp, DriverConfig(setupConfig, test, config.getInt("stingray.testIterations")))
    println("Starting test")
    Await.result(stingrayDriver.execute(), Duration.Inf)
    println("Test finished")
  }

  def buildDirtyWriteTest(schema: DataSchema, isolationLevel: IsolationLevels.Value): Test = {
    DirtyWrite(
      TestColumn("x", "litmustest0", 0),
      TestColumn("y", "litmustest0", 0),
      schema,
      isolationLevel)
  }

  def buildWriteSkewTest(schema: DataSchema, isolationLevel: IsolationLevels.Value): Test = {
    WriteSkew(
      TestColumn("x", "litmustest0", 0),
      "r0",
      TestColumn("y", "litmustest1", 0),
      "r1",
      schema,
      isolationLevel)
  }
}
