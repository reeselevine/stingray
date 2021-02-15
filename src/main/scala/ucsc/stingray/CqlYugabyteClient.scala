package ucsc.stingray

import com.datastax.driver.core.{Cluster, ResultSet}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import ucsc.stingray.YugabyteClient.{DataRow, DataValue, IntDataValue, StringDataValue}
import ucsc.stingray.sqldsl._

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters.CollectionHasAsScala

class CqlYugabyteClient extends YugabyteClient {

  import CqlYugabyteClient._
  val Keyspace = "stingray"

  val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
  val session = cluster.connect
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $Keyspace;")

  override def execute(createTableRequest: CreateTableRequest): Future[Unit] = {
    val createTableHeader = s"CREATE TABLE IF NOT EXISTS $Keyspace.${createTableRequest.tableName} ("
    val createTableFooter = ") with transactions = { 'enabled' : true };"
    val primaryKeyStr = s"${buildFieldDefinition(createTableRequest.primaryKey)} PRIMARY KEY"
    val otherFields = createTableRequest.schema.map(buildFieldDefinition).toSeq
    val joinedFields = (Seq(primaryKeyStr) ++ otherFields).mkString(",")
    session.executeAsync(createTableHeader + joinedFields + createTableFooter).asScala.map(_ => {})
  }

  override def execute(dropTableRequest: DropTableRequest): Future[Unit] = {
    session.executeAsync(s"DROP TABLE $Keyspace.${dropTableRequest.tableName}").asScala.map(_ => {})
  }

  override def execute(upsertOperation: Upsert): Future[Unit] = {
    val query = upsertOperation match {
      case update: Update => buildUpdate(update)
      case upsert => buildUpsert(upsert)
    }
    session.executeAsync(query).asScala.map(_ => {})
  }

  override def execute(transaction: Transaction): Future[Unit] = {
    val operations = transaction.operations.map { operation =>
      operation match {
        case update: Update => buildUpdate(update)
        case upsert: Upsert => buildUpsert(upsert)
      }
    }.mkString(" ")
    session.executeAsync(s"BEGIN TRANSACTION $operations END TRANSACTION;").asScala.map(_ => {})
  }

  override def execute(select: Select, schema: DataSchema): Future[Seq[DataRow]] = {
    val columns = select.columns match {
      case Right(values) => values.mkString(",")
      case Left(_) => "*"
    }
    val query = s"SELECT $columns FROM $Keyspace.${select.tableName}".withCondition(select.condition)
    session.executeAsync(query).asScala.map { resultSet =>
      var i = 0
      resultSet.all().asScala.map { row =>
        val data = schema.schema.foldLeft(Map[String, DataValue]()) {
          case (curMap, (key, dataType)) => dataType match {
            case DataTypes.Integer => curMap + (key -> IntDataValue(row.getInt(key)))
            case DataTypes.String => curMap + (key -> StringDataValue(row.getString(key)))
          }
        }
        val dataRow = DataRow(i, data)
        i += 1
        dataRow
      }.toSeq
    }
  }

  private def buildFieldDefinition(field: (String, DataTypes.Value)): String = field match {
    case (key, dataType) => dataType match {
      case DataTypes.Integer => s"$key int"
    }
  }

  private def buildUpdate(update: Update): String = {
    val updates = update.values.map {
      case (field, value) => s"$field = $value"
    }.mkString(",")
    s"UPDATE $Keyspace.${update.tableName} SET $updates".withCondition(update.condition)
  }

  private def buildUpsert(upsert: Upsert): String = {
    val fields = s"(${upsert.values.map(_._1).mkString(",")})"
    val values = s"(${upsert.values.map(_._2).mkString(",")})"
    s"INSERT INTO $Keyspace.${upsert.tableName} $fields VALUES $values".withCondition(upsert.condition)
  }

  def close(): Unit = {
    session.execute(s"DROP KEYSPACE $Keyspace")
    session.close()
    cluster.close()
  }
}

object CqlYugabyteClient {

  def apply(): CqlYugabyteClient = new CqlYugabyteClient()

  implicit class RichString(str: String) {
    def withCondition(condition: Option[String]) = {
      condition match {
        case Some(cond) => str + " WHERE " + cond + ";"
        case None => str + ";"
      }
    }
  }

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
