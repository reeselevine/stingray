package ucsc.stingray

import ucsc.stingray.sqllikedisl.IsolationLevels.IsolationLevel

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class StingrayDriver(app: StingrayApp) {

  def execute(): Future[Unit] = {
    for {
      _ <- app.setup()
      _ <- run()
      _ <- app.teardown()
    } yield {}
  }

  private def run(): Future[Unit] = {
    run(100, Map()).map { results =>
      println(results)
    }
  }

  private def run(iterationsLeft: Int, results: Map[IsolationLevel, Int]): Future[Map[IsolationLevel, Int]] = {
    iterationsLeft match {
      case 0 => Future(results)
      case _ => app.run().flatMap { result =>
        val curValue = results.getOrElse(result.serializationLevel, 0)
        val newResults = results + (result.serializationLevel -> (curValue + 1))
        run(iterationsLeft - 1, newResults)
      }
    }
  }
}

object StingrayDriver {

  def apply(app: StingrayApp): StingrayDriver = new StingrayDriver(app)
}
