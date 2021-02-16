package ucsc.stingray.sqllikedisl

/** Extending this trait gives access to the SQL DSL */
trait SqlLikeDsl {

  def createTable(tableName: String, primaryKey: (String, DataTypes.Value)): CreateTableRequest =
    CreateTableRequest(tableName, primaryKey)

  def dropTable(tableName: String): DropTableRequest = DropTableRequest(tableName)

  def insertInto(tableName: String): Insert = Insert(tableName)

  def update(tableName: String): Update = Update(tableName)

  def select(tableName: String): Select = Select(tableName)

  def transaction(): Transaction = Transaction()
}

/** Importing this object gives access to the SQL DSL */
object SqlLikeDsl extends SqlLikeDsl

/** Represents an operation to create a table. */
case class CreateTableRequest(tableName: String, primaryKey: (String, DataTypes.Value), schema: Map[String, DataTypes.Value] = Map()) {

  def withSchema(newSchema: Map[String, DataTypes.Value]) = copy(schema = newSchema)

  def withField(field: (String, DataTypes.Value)) = copy(schema = schema + field)
}

/** Represents an operation to drop a table. */
case class DropTableRequest(tableName: String)


object IsolationLevels extends Enumeration {
  type IsolationLevel = Value
  val Serializable, SnapshotIsolation, Nada = Value
}


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
case class Update(tableName: String, condition: Option[String] = None, values: Seq[(String, Any)] = Nil)
  extends Upsert {
  def withCondition(cond: String) = copy(condition = Some(cond))
  def withValues(values: Seq[(String, Any)]) = copy(values = values)
}

/** Represents a select operation. By default all columns are selected. */
case class Select(tableName: String, condition: Option[String] = None, columns: Either[All.type, Seq[String]] = Left(All))
  extends DataOperation {

  def withColumns(cols: Seq[String]) = copy(columns = Right(cols))

  def withCondition(cond: String) = copy(condition = Some(cond))
}

/** Represents '*' in SQL syntax, to return all columns */
case object All

/** Types of data that can be in a row. */
object DataTypes extends Enumeration {
  type DataType = Value

  val Integer, String = Value
}
/** Represents the expected schema of the data that should be returned by a query. */
case class DataSchema(schema: Map[String, DataTypes.Value])
