package ucsc.stingray

import ucsc.stingray.StingrayApp.DataSchema
import ucsc.stingray.YugabyteClient.DataRow

import scala.concurrent.Future

/** A client for executing queries on YugabyteDb */
trait YugabyteClient {

  /** Executes a query, and if a schema is defined, returns a
   * list of rows with data values matching the provided schema.
   * If the schema is not defined, returns an empty list. */
  def execute(query: String, schemaOpt: Option[DataSchema] = None): Future[Seq[DataRow]]

  /** Close the client */
  def close(): Unit
}

object YugabyteClient {

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
