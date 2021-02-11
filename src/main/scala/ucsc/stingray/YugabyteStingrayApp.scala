package ucsc.stingray

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

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
    yugabyteClient.execute(s"SELECT * FROM $Keyspace.$TestTableName").map { results =>
      results.all().asScala.map { row =>
        println(s"row: ${row.getInt(0)}, x: ${row.getInt(1)}, y: ${row.getInt(2)}")
      }
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
    Future.sequence((0 to 1).map { id =>
      val insert = s"""
        INSERT INTO $Keyspace.$TestTableName
                      |(id, x, y)
                      |VALUES
                      |($id, 0, 0);
        """.trim.stripMargin('|').replaceAll("\n", " ")
      yugabyteClient.execute(insert)
    }).map(_ => {})

  }
}

object YugabyteStingrayApp {

  def apply(yugabyteClient: YugabyteClient): YugabyteStingrayApp = new YugabyteStingrayApp(yugabyteClient)
}