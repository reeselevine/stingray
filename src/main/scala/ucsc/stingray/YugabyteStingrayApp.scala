package ucsc.stingray


import ucsc.stingray.StingrayApp.TestTypes
import ucsc.stingray.StingrayApp.{Result, TestConfig}
import ucsc.stingray.StingrayDriver.SerializationLevels

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class YugabyteStingrayApp(yugabyteClient: YugabyteClient, val config: TestConfig) extends StingrayApp {

  val Keyspace = "stingray"
  val TestTableName = "litmustest"

  def setup(): Future[Unit] = {
    for {
      _ <- createKeyspace()
      _ <- createTable()
    } yield {}
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
      s"""
         |BEGIN TRANSACTION
         |UPDATE $Keyspace.$TestTableName SET $dest = $source WHERE id = 0;
         |END TRANSACTION;
         |""".stripMargin.replaceAll("\n", " ")).map(_ => {})
  }

  private def buildDirtyWriteTransaction(value: Int): Future[Unit] = {
    yugabyteClient.execute(
      s"""
         |BEGIN TRANSACTION
         |UPDATE $Keyspace.$TestTableName SET x = $value WHERE id = 0;
         |UPDATE $Keyspace.$TestTableName SET y = $value WHERE id = 0;
         |END TRANSACTION;
         |""".stripMargin.replaceAll("\n", " ")).map(_ => {})
  }

  private def checkRow(): Future[(Int, Int)] = {
    yugabyteClient.execute(s"SELECT * FROM $Keyspace.$TestTableName WHERE id = 0", Some(config.dataSchema)).map { res =>
      val row = res(0)
      val x = row.data("x").intValue()
      val y = row.data("y").intValue()
      println(s"results: x: $x y: $y")
      (x, y)
    }
  }

  def teardown(): Future[Unit] = {
    for {
      _ <- yugabyteClient.execute(s"DROP TABLE $Keyspace.$TestTableName")
      _ <- yugabyteClient.execute(s" DROP KEYSPACE $Keyspace")
    } yield {
      yugabyteClient.close()
    }
  }

  private def createKeyspace(): Future[Unit] = {
    yugabyteClient.execute(s"CREATE KEYSPACE IF NOT EXISTS $Keyspace;").map(_ => {})
  }

  private def createTable(): Future[Unit] = {
    val createTable = s"""
        CREATE TABLE IF NOT EXISTS $Keyspace.$TestTableName (
          id int PRIMARY KEY,
          x int,
          y int
        ) with transactions = { 'enabled' : true };
      """
    yugabyteClient.execute(createTable).map(_ => {})
  }

  private def insertData(): Future[Unit] = {
    val insert = s"""
       INSERT INTO $Keyspace.$TestTableName
                     |(id, x, y)
                     |VALUES
                     |(0, 0, 1);
       """.trim.stripMargin('|').replaceAll("\n", " ")
    yugabyteClient.execute(insert).map(_ => {})
  }
}

object YugabyteStingrayApp {

  case class Results(serializable: Int, snapshot: Int)

  def apply(yugabyteClient: YugabyteClient, config: TestConfig): YugabyteStingrayApp = new YugabyteStingrayApp(yugabyteClient, config)
}