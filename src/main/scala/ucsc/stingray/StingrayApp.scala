package ucsc.stingray

import scala.concurrent.Future

trait StingrayApp {

  def setup(): Future[Unit]

  def run(): Future[Unit]

  def teardown(): Future[Unit]

}
