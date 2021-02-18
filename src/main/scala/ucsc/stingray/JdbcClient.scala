package ucsc.stingray

import org.postgresql.util.PSQLException

import java.sql.{Connection, ResultSet, Statement}
import ucsc.stingray.SqlLikeClient.{DataRow, DataValue, IntDataValue, StringDataValue}
import ucsc.stingray.StingrayApp.{DataSchema, DataTypes}
import ucsc.stingray.StingrayDriver.IsolationLevels
import ucsc.stingray.sqllikedisl.{CreateTableRequest, DropTableRequest, Select, SqlLikeUtils, Transaction, Upsert}

import scala.concurrent.blocking
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class JdbcClient(connectionPool: JdbcConnectionPool) extends SqlLikeClient with SqlLikeUtils {

  override def execute(createTableRequest: CreateTableRequest): Future[Unit] = {
    val createTableHeader = s"CREATE TABLE IF NOT EXISTS ${createTableRequest.tableName} ("
    val createTableFooter = ");"
    executeUpdate(buildTableSchema(createTableHeader, createTableFooter, createTableRequest), IsolationLevels.Serializable)
  }

  override def execute(dropTableRequest: DropTableRequest): Future[Unit] = {
    executeUpdate(s"DROP TABLE ${dropTableRequest.tableName};", IsolationLevels.Serializable)
  }

  override def execute(upsertOperation: Upsert): Future[Unit] = {
    executeUpdate(buildUpsert(upsertOperation.tableName, upsertOperation), IsolationLevels.Serializable)
  }

  override def execute(transaction: Transaction): Future[Unit] = {
    val operations = transaction.operations.map {
        case upsert: Upsert => buildUpsert(upsert.tableName, upsert)
    }.mkString(" ")
    executeUpdate(operations, transaction.isolationLevel)
  }

  override def execute(select: Select, schema: DataSchema): Future[Seq[DataRow]] = withConnection() { stmt =>
    Future {
      blocking {
        stmt.executeQuery(buildSelect(select.tableName, select))
      }
    } map { resultSet =>
      var i = 0
      var dataRows = Seq[DataRow]()
      while (resultSet.next()) {
        val data = select.columns match {
          case Right(columns) => columns.foldLeft(Map[String, DataValue]()) {
            case (curMap, key) => curMap + (key -> getDatum(key, schema.columns(key), resultSet))
          }
          case Left(_) => schema.columns.foldLeft(Map[String, DataValue]()) {
            case (curMap, (key, dataType)) => curMap + (key -> getDatum(key, dataType, resultSet))
          }
        }
        val dataRow = DataRow(i, data)
        i += 1
        dataRows = dataRows :+ dataRow
      }
      dataRows
    }
  }

  private def getDatum(key: String, dataType: DataTypes.Value, resultSet: ResultSet): DataValue = {
    dataType match {
      case DataTypes.Integer => IntDataValue(resultSet.getInt(key))
      case DataTypes.String => StringDataValue(resultSet.getString(key))
    }
  }

  private def executeUpdate(query: String, isolationLevel: IsolationLevels.Value): Future[Unit] =
    withConnection(isolationLevel) { stmt =>
      Future {
        blocking {
          stmt.executeUpdate(query)
        }
      }
    }

  override def close(): Unit = {}

  def withConnection[T](isolationLevel: IsolationLevels.Value = IsolationLevels.Serializable)
                       (block: Statement => Future[T]) = {
    val connection = connectionPool.getConnection()
    connection.setAutoCommit(false)
    connection.setTransactionIsolation(IsolationLevels.jdbcValue(isolationLevel))
    val stmt = connection.createStatement()
    withRetries(connection, retries = 3)(block(stmt)) andThen { result =>
      result match {
        case Success(_) => connection.commit()
        case Failure(_) => connection.rollback()
      }
      stmt.close()
      connection.close()
    }
  }

  def withRetries[T](connection: Connection, retries: Int)(block: => Future[T]): Future[T] = {
    if (retries > 0) {
      block recoverWith {
        case _: PSQLException =>
          connection.rollback()
          withRetries(connection, retries - 1)(block)
      }
    } else block
  }
}

object JdbcClient {

  def apply(jdbcConnectionPool: JdbcConnectionPool): JdbcClient = new JdbcClient(jdbcConnectionPool)
}
