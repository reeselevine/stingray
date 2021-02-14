package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.SerializationLevels.SerializationLevel

import scala.concurrent.Future

trait StingrayApp {

  def config: TestConfig

  def setup(): Future[Unit]

  def run(): Future[Result]

  def teardown(): Future[Unit]
}

object StingrayApp {

  object TestTypes extends Enumeration {
    type TestType = Value

    val WriteSkew, DirtyWrite = Value
  }

  /** Types of data that can be in a row. */
  object DataTypes extends Enumeration {
    type DataType = Value

    val Integer, String = Value
  }
  /** Represents the expected schema of the data that should be returned by a query. */
  case class DataSchema(schema: Map[String, DataTypes.Value])

  case class TestConfig(testType: TestTypes.Value, dataSchema: DataSchema)
  case class Result(serializationLevel: SerializationLevel)
}
