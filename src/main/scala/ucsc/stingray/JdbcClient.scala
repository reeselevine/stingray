package ucsc.stingray

import org.postgresql.util.PSQLException

import java.sql.Statement
import ucsc.stingray.SqlLikeClient.{DataRow, DataValue, IntDataValue, StringDataValue}
import ucsc.stingray.sqllikedisl.{CreateTableRequest, DataSchema, DataTypes, DropTableRequest, IsolationLevels, Select, SqlLikeUtils, Transaction, Upsert}

import scala.concurrent.blocking
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class JdbcClient(connectionPool: JdbcConnectionPool) extends SqlLikeClient with SqlLikeUtils {

  override def execute(createTableRequest: CreateTableRequest): Future[Unit] = {
    val createTableHeader = s"CREATE TABLE IF NOT EXISTS ${createTableRequest.tableName} ("
    val createTableFooter = ");"
    executeUpdate(buildTableSchema(createTableHeader, createTableFooter, createTableRequest))
  }

  override def execute(dropTableRequest: DropTableRequest): Future[Unit] = {
    executeUpdate(s"DROP TABLE ${dropTableRequest.tableName};")
  }

  override def execute(upsertOperation: Upsert): Future[Unit] = {
    executeUpdate(buildUpsert(upsertOperation.tableName, upsertOperation))
  }

  override def execute(transaction: Transaction): Future[Unit] = {
    val isolationLevel = transaction.isolationLevel match {
      case IsolationLevels.Serializable => "SERIALIZABLE"
      case IsolationLevels.SnapshotIsolation => "REPEATABLE READ"
    }
    val operations = transaction.operations.map { operation =>
      operation match {
        case upsert: Upsert => buildUpsert(operation.tableName, upsert)
      }
    }.mkString(" ")
    executeUpdate(s"BEGIN TRANSACTION; SET TRANSACTION ISOLATION LEVEL $isolationLevel; $operations END;")
  }

  override def execute(select: Select, schema: DataSchema): Future[Seq[DataRow]] = withConnection { stmt =>
    Future {
      blocking {
        stmt.executeQuery(buildSelect(select.tableName, select))
      }
    } map { resultSet =>
      var i = 0
      var dataRows = Seq[DataRow]()
      while (resultSet.next()) {
        val data = schema.schema.foldLeft(Map[String, DataValue]()) {
          case (curMap, (key, dataType)) => dataType match {
            case DataTypes.Integer => curMap + (key -> IntDataValue(resultSet.getInt(key)))
            case DataTypes.String => curMap + (key -> StringDataValue(resultSet.getString(key)))
          }
        }
        val dataRow = DataRow(i, data)
        i += 1
        dataRows = dataRows :+ dataRow
      }
      dataRows
    }
  }

  private def executeUpdate(query: String): Future[Unit] = withConnection { stmt =>
    withRetries() {
      Future {
        blocking {
          stmt.executeUpdate(query)
        }
      }
    }
  }

  override def close(): Unit = {}

  def withConnection[T](block: Statement => Future[T]) = {
    val connection = connectionPool.getConnection()
    val stmt = connection.createStatement()
    block(stmt) andThen { _ =>
      stmt.close()
      connection.close()
    }
  }

  def withRetries[T](retries: Int = 10)(block: => Future[T]): Future[T] = {
    if (retries > 0)
      block recoverWith {
        case _: PSQLException => println("retrying")
          withRetries(retries - 1)(block)
      }
    else
      block
  }
}

object JdbcClient {

  def apply(jdbcConnectionPool: JdbcConnectionPool): JdbcClient = new JdbcClient(jdbcConnectionPool)
}
