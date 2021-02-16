package ucsc.stingray


import ucsc.stingray.StingrayApp.TestTypes
import ucsc.stingray.StingrayApp.{Result, TestConfig}
import ucsc.stingray.sqllikedisl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqlLikeStingrayApp(sqlLikeClient: SqlLikeClient, val config: TestConfig) extends StingrayApp
with SqlLikeDsl {

  val TestTableName = "litmustest"

  def setup(): Future[Unit] = {
    for {
      _ <- sqlLikeClient.execute(createTable(TestTableName, ("id", DataTypes.Integer)).withSchema(config.dataSchema.schema))
      _ <- sqlLikeClient.execute(insertInto(TestTableName).withValues(Seq(("id", 0), ("x", 0), ("y", 1))))
    } yield {}
  }

  override def run(): Future[Result] = {
    config.testType match {
      case TestTypes.WriteSkew => runWriteSkew()
      case TestTypes.DirtyWrite => runDirtyWrite()
    }

  }

  private def runDirtyWrite(): Future[Result] = {
    updateData().flatMap { _ =>
      val t1 = buildDirtyWriteTransaction(1)
      val t2 = buildDirtyWriteTransaction(2)
      for {
        _ <- t1
        _ <- t2
        (x, y) <- checkRow()
      } yield {
        if (x == y) {
          Result(IsolationLevels.Serializable)
        } else {
          Result(IsolationLevels.Nada)
        }
      }
    }
  }

  private def runWriteSkew(): Future[Result] = {
    updateData().flatMap { _ =>
      val t1 = buildWriteSkewTransaction("x", "y")
      val t2 = buildWriteSkewTransaction("y", "x")
      for {
        _ <- t1
        _ <- t2
        (x, y) <- checkRow()
      } yield {
        if (x == y) {
          Result(IsolationLevels.Serializable)
        } else {
          Result(IsolationLevels.SnapshotIsolation)
        }
      }
    }
  }

  private def buildWriteSkewTransaction(source: String, dest: String): Future[Unit] = {
    sqlLikeClient.execute(
      transaction()
        .add(update(TestTableName).withValues(Seq((dest, source))).withCondition("id = 0")))
  }

  private def buildDirtyWriteTransaction(value: Int): Future[Unit] = {
    sqlLikeClient.execute(
      transaction()
        .setIsolationLevel(IsolationLevels.Serializable)
        .add(update(TestTableName).withValues(Seq(("x", value))).withCondition("id = 0"))
        .add(update(TestTableName).withValues(Seq(("y" , value))).withCondition("id = 0")))
  }

  private def checkRow(): Future[(Int, Int)] = {
    sqlLikeClient.execute(select(TestTableName).withCondition("id = 0"), config.dataSchema).map { res =>
      val row = res(0)
      val x = row.data("x").intValue()
      val y = row.data("y").intValue()
      println(s"results: x: $x y: $y")
      (x, y)
    }
  }

  def teardown(): Future[Unit] = {
    for {
      _ <- sqlLikeClient.execute(dropTable(TestTableName))
    } yield {
      sqlLikeClient.close()
    }
  }

  private def updateData(): Future[Unit] = {
    sqlLikeClient.execute(update(TestTableName).withValues(Seq(("x", 0), ("y", 1))).withCondition("id = 0"))
  }
}

object SqlLikeStingrayApp {

  case class Results(serializable: Int, snapshot: Int)

  def apply(sqlLikeClient: SqlLikeClient, config: TestConfig): SqlLikeStingrayApp = new SqlLikeStingrayApp(sqlLikeClient, config)
}