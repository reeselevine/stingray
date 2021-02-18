package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.IsolationLevels

import scala.concurrent.Future

trait StingrayApp {

  def setup(setupConfig: SetupConfig): Future[Unit]

  def run(test: Test): Future[Result]

  def teardown(teardownConfig: TeardownConfig): Future[Unit]
}

object StingrayApp {

  sealed trait Test

  /**
   * Represents a test column for a test type.
   * @param field The name of the column to be tested.
   * @param table The table which the column resides in.
   * @param primaryKeyValue The primary key value of the row of the column to be tested.
   */
  case class TestColumn(field: String, resultField: String, table: String, primaryKeyValue: Int)

  /**
   * Tests whether the write skew phenomenon is allowed under the given isolation level.
   */
  case class WriteSkew(x: TestColumn, y: TestColumn, dataSchema: DataSchema) extends Test

  /** Types of data that can be in a row. */
  object DataTypes extends Enumeration {
    type DataType = Value

    val Integer, String = Value
  }
  /** Represents the expected schema of the data that should be returned by a query. */
  case class DataSchema(primaryKey: String, columns: Map[String, DataTypes.Value])

  case class TableSetup(schema: DataSchema, numRows: Int)

  case class SetupConfig(tables: Map[String, TableSetup])

  case class Result(serializationLevel: IsolationLevels.Value)

  case class TeardownConfig(tables: Seq[String])

}
