package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.SerializationLevels.SerializationLevel

import scala.concurrent.Future

trait StingrayApp {

  def setup(): Future[Unit]

  def run(config: TestConfig): Future[Result]

  def teardown(): Future[Unit]
}

object StingrayApp {

  object TestTypes extends Enumeration {
    type TestType = Value

    val WriteSkew, DirtyWrite = Value
  }

  case class TestConfig(testType: TestTypes.Value)
  case class Result(serializationLevel: SerializationLevel)
}
