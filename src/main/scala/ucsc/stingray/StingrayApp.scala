package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.SerializationLevels.SerializationLevel

import scala.concurrent.Future

trait StingrayApp {

  def setup(): Future[Unit]

  def run(): Future[Result]

  def teardown(): Future[Unit]
}

object StingrayApp {
  case class Result(serializationLevel: SerializationLevel)
}
