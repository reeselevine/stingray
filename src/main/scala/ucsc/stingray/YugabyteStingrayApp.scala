package ucsc.stingray


import ucsc.stingray.StingrayApp.TestTypes
import ucsc.stingray.StingrayApp.{Result, TestConfig}
import ucsc.stingray.sqldsl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class YugabyteStingrayApp(yugabyteClient: YugabyteClient, val config: TestConfig) extends StingrayApp
with SqlDsl {

  val Keyspace = "stingray"
  val TestTableName = "litmustest"

  def setup(): Future[Unit] = {
    yugabyteClient.execute(
      createTable(TestTableName, ("id", DataTypes.Integer))
        .withSchema(config.dataSchema.schema))
  }

  override def run(): Future[Result] = {
    config.testType match {
      case TestTypes.WriteSkew => runWriteSkew()
      case TestTypes.DirtyWrite => runDirtyWrite()
    }

  }

  private def runDirtyWrite(): Future[Result] = {
    insertData().flatMap { _ =>
      val t1 = buildDirtyWriteTransaction(1)
      val t2 = buildDirtyWriteTransaction(2)
      for {
        _ <- t1
        _ <- t2
        (x, y) <- checkRow()
      } yield {
        if (x == y) {
          Result(SerializationLevels.Serializable)
        } else {
          Result(SerializationLevels.Nada)
        }
      }
    }
  }

  private def runWriteSkew(): Future[Result] = {
    insertData().flatMap { _ =>
      val t1 = buildWriteSkewTransaction("x", "y")
      val t2 = buildWriteSkewTransaction("y", "x")
      for {
        _ <- t1
        _ <- t2
        (x, y) <- checkRow()
      } yield {
        if (x == y) {
          Result(SerializationLevels.Serializable)
        } else {
          Result(SerializationLevels.SnapshotIsolation)
        }
      }
    }
  }

  private def buildWriteSkewTransaction(source: String, dest: String): Future[Unit] = {
    yugabyteClient.execute(
      transaction()
        .add(update(TestTableName).withValues(Seq((dest, source))).withCondition("id = 0")))
  }

  private def buildDirtyWriteTransaction(value: Int): Future[Unit] = {
    yugabyteClient.execute(
      transaction()
        .add(update(TestTableName).withValues(Seq(("x", value))).withCondition("id = 0"))
        .add(update(TestTableName).withValues(Seq(("y" , value))).withCondition("id = 0")))
  }

  private def checkRow(): Future[(Int, Int)] = {
    yugabyteClient.execute(select(TestTableName).withCondition("id = 0"), config.dataSchema).map { res =>
      val row = res(0)
      val x = row.data("x").intValue()
      val y = row.data("y").intValue()
      println(s"results: x: $x y: $y")
      (x, y)
    }
  }

  def teardown(): Future[Unit] = {
    for {
      _ <- yugabyteClient.execute(dropTable(TestTableName))
    } yield {
      yugabyteClient.close()
    }
  }

  private def insertData(): Future[Unit] = {
    yugabyteClient.execute(insertInto(TestTableName).withValues(Seq(("id", 0), ("x", 0), ("y", 1))))
  }
}

object YugabyteStingrayApp {

  case class Results(serializable: Int, snapshot: Int)

  def apply(yugabyteClient: YugabyteClient, config: TestConfig): YugabyteStingrayApp = new YugabyteStingrayApp(yugabyteClient, config)
}