package ucsc.stingray

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import StingrayDriver._
import ucsc.stingray.StingrayApp.{SetupConfig, TeardownConfig, Test, TestFailedException}

import scala.util.control.NonFatal

class StingrayDriver(app: StingrayApp, config: DriverConfig) {

  def execute(): Future[Unit] = {
    for {
      _ <- app.setup(config.setupConfig)
      _ <- run()
      _ <- app.teardown(TeardownConfig(config.setupConfig.tables.keys.toSeq))
    } yield {}
  }

  private def run(): Future[Unit] = {
    run(config.iterations, Map()).map { results =>
      println(results)
    }
  }

  private def run(iterationsLeft: Int, results: Map[IsolationLevels.Value, Int]): Future[Map[IsolationLevels.Value, Int]] = {
    iterationsLeft match {
      case 0 => Future(results)
      case _ => app.run(config.test).flatMap { result =>
        val curValue = results.getOrElse(result.serializationLevel, 0)
        val newResults = results + (result.serializationLevel -> (curValue + 1))
        run(iterationsLeft - 1, newResults)
      } recoverWith {
        case TestFailedException(message) =>
          println(s"test run failed. Message: $message")
          Future(results)
        case NonFatal(e) =>
          println(s"test run failed. Reason: ${e.getMessage}")
          run(iterationsLeft - 1, results)
        case e =>
          println("what the heck")
          throw e
      }
    }
  }
}

object StingrayDriver {

  case class DriverConfig(
                           setupConfig: SetupConfig,
                           test: Test,
                           iterations: Int)

  object IsolationLevels extends Enumeration {
    type IsolationLevel = Value
    val Serializable, SnapshotIsolation, Nada = Value

    def jdbcValue(value: Value): Int = value match {
      case Serializable => 8
      case SnapshotIsolation => 4
      case Nada => 0
    }
  }
  def apply(app: StingrayApp, config: DriverConfig): StingrayDriver = new StingrayDriver(app, config)
}
