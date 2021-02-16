package ucsc.stingray

import ucsc.stingray.SqlLikeClient.DataRow
import ucsc.stingray.sqllikedisl._

import scala.concurrent.Future

/** A client for executing Sql Like queries.  */
trait SqlLikeClient {

  def execute(createTableRequest: CreateTableRequest): Future[Unit]

  def execute(dropTableRequest: DropTableRequest): Future[Unit]

  def execute(upsertOperation: Upsert): Future[Unit]

  def execute(select: Select, schema: DataSchema): Future[Seq[DataRow]]

  def execute(transaction: Transaction): Future[Unit]

  /** Close the client */
  def close(): Unit
}

object SqlLikeClient {

  /** Representation of a data type returned by a query. */
  sealed trait DataValue {
    def intValue(): Int
    def stringValue(): String
  }

  case class IntDataValue(value: Int) extends DataValue {
    override def intValue(): Int = value
    override def stringValue(): String = value.toString
  }

  case class StringDataValue(value: String) extends DataValue {
    override def intValue(): Int = throw new UnsupportedOperationException()
    override def stringValue(): String = value
  }

  /** Represents a single row returned by a query. */
  case class DataRow(rowId: Int, data: Map[String, DataValue])
}
