package ucsc.stingray

import ucsc.stingray.StingrayApp.{TestConfig, TestTypes}
import ucsc.stingray.sqllikedisl.{DataSchema, DataTypes}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {
    println("Starting test")
    val schema = DataSchema(Map("x" -> DataTypes.Integer, "y" -> DataTypes.Integer))
    val config = TestConfig(TestTypes.WriteSkew, schema)
    val sqlLikeClient = CqlYugabyteClient()
    val stingrayApp = SqlLikeStingrayApp(sqlLikeClient, config)
    val stingrayDriver = StingrayDriver(stingrayApp)
    Await.result(stingrayDriver.execute(), Duration.Inf)
    println("Test finished")
  }
}
