package ucsc.stingray

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class StingrayDriver(app: StingrayApp) {

  def execute(): Future[Unit] = {
    for {
      _ <- app.setup()
      _ <- app.run()
      _ <- app.teardown()
    } yield {}
  }
}

object StingrayDriver {

  def apply(app: StingrayApp): StingrayDriver = new StingrayDriver(app)
}
