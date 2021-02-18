package ucsc.stingray

import com.datastax.driver.core.{Cluster, ResultSet, Row}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import ucsc.stingray.SqlLikeClient.{DataRow, DataValue, IntDataValue, StringDataValue}
import ucsc.stingray.StingrayApp.{DataSchema, DataTypes}
import ucsc.stingray.sqllikedisl._

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters.CollectionHasAsScala

class CqlYugabyteClient extends SqlLikeClient with SqlLikeUtils {

  import CqlYugabyteClient._
  val Keyspace = "stingray"

  val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
  val session = cluster.connect
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $Keyspace;")

  override def execute(createTableRequest: CreateTableRequest): Future[Unit] = {
    val createTableHeader = s"CREATE TABLE IF NOT EXISTS $Keyspace.${createTableRequest.tableName} ("
    val createTableFooter = ") with transactions = { 'enabled' : true };"
    session.executeAsync(buildTableSchema(createTableHeader, createTableFooter, createTableRequest)).asScala.map(_ => {})
  }

  override def execute(dropTableRequest: DropTableRequest): Future[Unit] = {
    session.executeAsync(s"DROP TABLE $Keyspace.${dropTableRequest.tableName}").asScala.map(_ => {})
  }

  override def execute(upsertOperation: Upsert): Future[Unit] = {
    val tableName = s"$Keyspace.${upsertOperation.tableName}"
    session.executeAsync(buildUpsert(tableName, upsertOperation)).asScala.map(_ => {})
  }

  override def execute(transaction: Transaction): Future[Unit] = {
    val operations = transaction.operations.map { operation =>
      val tableName = s"$Keyspace.${operation.tableName}"
      operation match {
        case upsert: Upsert => buildUpsert(tableName, upsert)
      }
    }.mkString(" ")
    session.executeAsync(s"BEGIN TRANSACTION $operations END TRANSACTION;").asScala.map(_ => {})
  }

  override def execute(select: Select, schema: DataSchema): Future[Seq[DataRow]] = {
    session.executeAsync(buildSelect(s"$Keyspace.${select.tableName}", select)).asScala.map { resultSet =>
      var i = 0
      resultSet.all().asScala.map { row =>
        val data = select.columns match {
          case Right(columns) => columns.foldLeft(Map[String, DataValue]()) {
            case (curMap, key) => curMap + (key -> getDatum(key, schema.columns(key), row))
          }
          case Left(_) => schema.columns.foldLeft(Map[String, DataValue]()) {
            case (curMap, (key, dataType)) => curMap + (key -> getDatum(key, dataType, row))
          }
        }
        val dataRow = DataRow(i, data)
        i += 1
        dataRow
      }.toSeq
    }
  }

  private def getDatum(key: String, dataType: DataTypes.Value, row: Row): DataValue = {
    dataType match {
      case DataTypes.Integer => IntDataValue(row.getInt(key))
      case DataTypes.String => StringDataValue(row.getString(key))
    }
  }

  def close(): Unit = {
    session.execute(s"DROP KEYSPACE $Keyspace")
    session.close()
    cluster.close()
  }
}

object CqlYugabyteClient {

  def apply(): CqlYugabyteClient = new CqlYugabyteClient()

  implicit class RichListenableFuture(lf: ListenableFuture[ResultSet]) {
    def asScala: Future[ResultSet] = {
      val p = Promise[ResultSet]()
      Futures.addCallback(lf, new FutureCallback[ResultSet] {
        override def onFailure(t: Throwable): Unit = p.failure(t)
        override def onSuccess(result: ResultSet): Unit = p.success(result)
      })
      p.future
    }
  }
}
