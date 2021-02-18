package ucsc.stingray.sqllikedisl

import ucsc.stingray.StingrayApp.DataSchema
import ucsc.stingray.StingrayDriver.IsolationLevels

/** Extending this trait gives access to the SQL DSL */
trait SqlLikeDsl {

  def createTable(tableName: String, schema: DataSchema): CreateTableRequest = CreateTableRequest(tableName, schema)

  def dropTable(tableName: String): DropTableRequest = DropTableRequest(tableName)

  def insertInto(tableName: String): Insert = Insert(tableName)

  def update(tableName: String): Update = Update(tableName)

  def select(tableName: String): Select = Select(tableName)

  def transaction(): Transaction = Transaction()
}

/** Importing this object gives access to the SQL DSL */
object SqlLikeDsl extends SqlLikeDsl

/** Represents an operation to create a table. */
case class CreateTableRequest(tableName: String, schema: DataSchema)

/** Represents an operation to drop a table. */
case class DropTableRequest(tableName: String)

/** Represents a transaction, which is a list of data operations. */
case class Transaction(isolationLevel: IsolationLevels.Value = IsolationLevels.SnapshotIsolation, operations: Seq[DataOperation] = Nil) {

  def add(operation: DataOperation): Transaction = copy(operations = operations :+ operation)

  def setIsolationLevel(isolationLevel: IsolationLevels.Value): Transaction = copy(isolationLevel = isolationLevel)
}

/** Represents an operation on data, such as an insert, update, or select. */
trait DataOperation {
  def tableName: String
  def condition: Option[String]
}

trait Upsert extends DataOperation {
  def values: Seq[(String, Any)]
  def withCondition(cond: String): Upsert
  def withValues(values: Seq[(String, Any)]): Upsert
}

/** Represents an insert operation. */
case class Insert(tableName: String, condition: Option[String] = None, values: Seq[(String, Any)] = Nil)
  extends Upsert {
  def withCondition(cond: String) = copy(condition = Some(cond))
  def withValues(values: Seq[(String, Any)]) = copy(values = values)
}

/** Represents an update operation. */
case class Update(
                   tableName: String,
                   condition: Option[String] = None,
                   values: Seq[(String, Any)] = Nil,
                   selectUpdateOpt: Option[Select] = None) extends Upsert {
  def withCondition(cond: String) = copy(condition = Some(cond))
  def withValues(values: Seq[(String, Any)]) = copy(values = values)
  def withSelectUpdate(select: Select) = copy(selectUpdateOpt = Some(select))
}

/** Represents a select operation. By default all columns are selected. */
case class Select(tableName: String, condition: Option[String] = None, columns: Either[All.type, Seq[String]] = Left(All))
  extends DataOperation {

  def withColumns(cols: Seq[String]) = copy(columns = Right(cols))

  def withCondition(cond: String) = copy(condition = Some(cond))
}

/** Represents '*' in SQL syntax, to return all columns */
case object All