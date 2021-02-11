package ucsc.stingray


import ucsc.stingray.YugabyteStingrayApp.Results

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class YugabyteStingrayApp(yugabyteClient: YugabyteClient) extends StingrayApp {

  val Keyspace = "stingray"
  val TestTableName = "litmustest"

  def setup(): Future[Unit] = {
    for {
      _ <- createKeyspace()
      _ <- createTable()
      _ <- insertData()
    } yield {}
  }

  def run(): Future[Unit] = {
    run(Results(0, 0), 100).map { results =>
      println(s"Serializable: ${results.serializable}, Snapshot: ${results.snapshot}")
    }
  }

  def run(curResults: Results, iterationsLeft: Int): Future[Results] = {
    iterationsLeft match {
      case 0 => Future(curResults)
      case i =>
        val t1 = setValue("x", "y")
        val t2 = setValue("y", "x")
        for {
          _ <- t1
          _ <- t2
          (x, y) <- checkRow("after")
          newResults = if (x == y)
            curResults.copy(serializable = curResults.serializable + 1)
          else
            curResults.copy(snapshot = curResults.snapshot + 1)
          _ <- insertData()
          finalResults <- run(newResults, i - 1)
        } yield finalResults
    }
  }

  private def setValue(source: String, dest: String): Future[Unit] = {
    yugabyteClient.execute(
      s"""
         |BEGIN TRANSACTION
         |UPDATE $Keyspace.$TestTableName SET $dest = $source WHERE id = 0;
         |END TRANSACTION;
         |""".stripMargin.replaceAll("\n", " ")).map(_ => {})
  }

  private def checkRow(state: String): Future[(Int, Int)] = {
    yugabyteClient.execute(s"SELECT * FROM $Keyspace.$TestTableName WHERE id = 0").map { res =>
      val row = res.all().get(0)
      println(s"$state: x: ${row.getInt(1)}, y: ${row.getInt(2)}")
      (row.getInt(1), row.getInt(2))
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

  def apply(yugabyteClient: YugabyteClient): YugabyteStingrayApp = new YugabyteStingrayApp(yugabyteClient)
}