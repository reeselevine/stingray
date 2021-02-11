package ucsc.stingray

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class YugabyteStingrayDriver(yugabyteClient: YugabyteClient) {

  val Keyspace = "stingray"

  def setup(): Future[Unit] = {
    val table = "litmustest"
    for {
      _ <- createKeyspace()
      _ <- createTable(table)
      _ <- insertData(table)
    } yield {}
  }

  private def createKeyspace(): Future[Unit] = {
    yugabyteClient.execute(s"CREATE KEYSPACE IF NOT EXISTS $Keyspace;").map(_ => {})
  }

  private def createTable(table: String): Future[Unit] = {
    val createTable = s"""
        CREATE TABLE IF NOT EXISTS $Keyspace.$table (
          id int PRIMARY KEY,
          x int,
          y int
        ) with transactions = { 'enabled' : true };
      """
    yugabyteClient.execute(createTable).map(_ => {})
  }

  private def insertData(table: String): Future[Unit] = {
    Future.sequence((0 to 1).map { id =>
      val insert = s"""
        INSERT INTO $Keyspace.$table
                      |(id, x, y)
                      |VALUES
                      |($id, 0, 0);
        """.trim.stripMargin('|').replaceAll("\n", " ")
      yugabyteClient.execute(insert)
    }).map(_ => {})

  }
}