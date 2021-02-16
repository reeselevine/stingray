package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.sqllikedisl.DataSchema
import ucsc.stingray.sqllikedisl.IsolationLevels.IsolationLevel

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

  case class TestConfig(testType: TestTypes.Value, dataSchema: DataSchema)
  case class Result(serializationLevel: IsolationLevel)
}
