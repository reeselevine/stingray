package ucsc.stingray

import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.IsolationLevels
import ucsc.stingray.sqllikedisl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqlLikeStingrayApp(sqlLikeClient: SqlLikeClient) extends StingrayApp
with SqlLikeDsl {

  override def setup(setupConfig: SetupConfig): Future[Unit] = {
    Future.sequence(setupConfig.tables.map {
      case (tableName, tableSetup) => setupTable(tableName, tableSetup)
    }).map(_ => {})
  }

  override def run(test: Test): Future[Result] = {
    test match {
      case writeSkew: WriteSkew => runWriteSkew(writeSkew)
      case dirtyWrite: DirtyWrite => runDirtyWrite(dirtyWrite)
    }
  }

  override def teardown(teardownConfig: TeardownConfig): Future[Unit] = {
    teardownConfig.tables.foldLeft(Future({})) { (result, table) =>
      result.flatMap(_ => sqlLikeClient.execute(dropTable(table)))
    } map(_ => sqlLikeClient.close())
  }

  private def runDirtyWrite(test: DirtyWrite): Future[Result] = {
    for {
      _ <- initializeData(test.x.table, test.dataSchema, test.x.primaryKeyValue)
      _ <- initializeData(test.y.table, test.dataSchema, test.y.primaryKeyValue)
      t1 = buildDirtyWriteTransaction(test.x, test.y, test)
      t2 = buildDirtyWriteTransaction(test.y, test.x, test)
      _ <- t1
      _ <- t2
      x <- getValue(test.x.table, test.dataSchema, test.x.field, test.x.primaryKeyValue)
      y <- getValue(test.y.table, test.dataSchema, test.y.field, test.y.primaryKeyValue)
    } yield {
      println(s"result: x = $x, y = $y")
      if (x == 1 && y == 1) {
        Result(IsolationLevels.Nada)
      } else {
        Result(IsolationLevels.Serializable)
      }
    }
  }

  private def runWriteSkew(test: WriteSkew): Future[Result] = {
    for {
      _ <- initializeData(test.x.table, test.dataSchema, test.x.primaryKeyValue)
      _ <- initializeData(test.y.table, test.dataSchema, test.y.primaryKeyValue)
      t1 = buildWriteSkewTransaction(test.x, test.y, test.yResultField, test)
      t2 = buildWriteSkewTransaction(test.y, test.x, test.xResultField, test)
      _ <- t1
      _ <- t2
      r0 <- getValue(test.x.table, test.dataSchema, test.xResultField, test.x.primaryKeyValue)
      r1 <- getValue(test.y.table, test.dataSchema, test.yResultField, test.y.primaryKeyValue)
      x <- getValue(test.x.table, test.dataSchema, test.x.field, test.x.primaryKeyValue)
      y <- getValue(test.y.table, test.dataSchema, test.y.field, test.y.primaryKeyValue)
    } yield {
      println(s"result: r0 = $r0, r1 = $r1")
      if (x != 1 || y != 1) {
        throw TestFailedException(s"x: $x, y: $y, both must equal 1")
      }
      if (r0 == 0 && r1 == 0) {
        Result(IsolationLevels.SnapshotIsolation)
      } else if (r0 == 1 && r1 == 1) {
        Result(IsolationLevels.Nada)
      } else {
        Result(IsolationLevels.Serializable)
      }
    }
  }

  private def buildWriteSkewTransaction(
                                         myColumn: TestColumn,
                                         otherColumn: TestColumn,
                                         resultField: String,
                                         test: WriteSkew): Future[Unit] = {
    sqlLikeClient.execute(transaction()
      .setIsolationLevel(test.isolationLevel)
      .add(update(myColumn.table)
        .withValues(Seq((myColumn.field, 1)))
        .withCondition(s"${test.dataSchema.primaryKey} = ${myColumn.primaryKeyValue}"))
      .add(update(otherColumn.table)
        .withValues(Seq((resultField, otherColumn.field)))
        .withCondition(s"${test.dataSchema.primaryKey} = ${otherColumn.primaryKeyValue}")))
  }

  private def buildDirtyWriteTransaction(
                                          myColumn: TestColumn,
                                          otherColumn: TestColumn,
                                          test: DirtyWrite): Future[Unit] = {
    sqlLikeClient.execute(transaction()
      .setIsolationLevel(test.isolationLevel)
      .add(update(myColumn.table)
        .withValues(Seq((myColumn.field, 2)))
        .withCondition(s"${test.dataSchema.primaryKey} = ${myColumn.primaryKeyValue}"))
      .add(update(otherColumn.table)
        .withValues(Seq((otherColumn.field, 1)))
        .withCondition(s"${test.dataSchema.primaryKey} = ${otherColumn.primaryKeyValue}")))
  }

  private def getValue(tableName: String, schema: DataSchema, column: String, primaryKeyValue: Int): Future[Int] = {
    sqlLikeClient.execute(select(tableName)
      .withColumns(Seq(column))
      .withCondition(s"${schema.primaryKey} = $primaryKeyValue"), schema)
      .map(_.head.data(column).intValue())
  }

  private def initializeData(tableName: String, schema: DataSchema, primaryKeyValue: Int): Future[Unit] = {
    val values: Seq[(String, Any)] = schema.columns.map {
      case (column, dataType) => dataType match {
        case DataTypes.Integer => (column, 0)
        case DataTypes.String => (column, "''")
      }
    }.toSeq
    sqlLikeClient.execute(update(tableName).withValues(values)
      .withCondition(s"${schema.primaryKey} = $primaryKeyValue"))
  }

  private def setupTable(tableName: String, tableSetup: TableSetup): Future[Unit] = {
    sqlLikeClient.execute(createTable(tableName, tableSetup.schema)) flatMap { _ =>
      val values: Seq[(String, Any)] = tableSetup.schema.columns.map(column => (column._1, "NULL")).toSeq
      Future.sequence((0 until tableSetup.numRows) map { i =>
        sqlLikeClient.execute(insertInto(tableName).withValues(values :+ (tableSetup.schema.primaryKey, i)))
      })
    } map(_ => {})
  }
}

object SqlLikeStingrayApp {

  def apply(sqlLikeClient: SqlLikeClient): SqlLikeStingrayApp = new SqlLikeStingrayApp(sqlLikeClient)
}