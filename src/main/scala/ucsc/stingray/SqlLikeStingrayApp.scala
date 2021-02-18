package ucsc.stingray


import ucsc.stingray.StingrayApp._
import ucsc.stingray.StingrayDriver.IsolationLevels
import ucsc.stingray.sqllikedisl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SqlLikeStingrayApp(sqlLikeClient: SqlLikeClient) extends StingrayApp
with SqlLikeDsl {

  val TestTableName0 = "litmustest0"
  val TestTableName1 = "litmustest1"
  val PrimaryKeyField = "id"

  override def setup(setupConfig: SetupConfig): Future[Unit] = {
    Future.sequence(setupConfig.tables.map {
      case (tableName, tableSetup) => setupTable(tableName, tableSetup)
    }).map(_ => {})
  }

  override def run(test: Test): Future[Result] = {
    test match {
      case writeSkew: WriteSkew => runWriteSkew(writeSkew)
    }
  }

  override def teardown(teardownConfig: TeardownConfig): Future[Unit] = {
    Future.sequence(teardownConfig.tables.map(table => sqlLikeClient.execute(dropTable(table))))
      .map(_ => sqlLikeClient.close())
  }
  /*

    private def runDirtyWrite(): Future[Result] = {
      updateData().flatMap { _ =>
        val t1 = buildDirtyWriteTransaction(1)
        val t2 = buildDirtyWriteTransaction(2)
        for {
          _ <- t1
          _ <- t2
          (x, y) <- checkRow(TestTableName0)
        } yield {
          if (x == y) {
            Result(IsolationLevels.Serializable)
          } else {
            Result(IsolationLevels.Nada)
          }
        }
      }
    }
  */

  private def runWriteSkew(test: WriteSkew): Future[Result] = {
    for {
      _ <- initializeData(test.x.table, test.dataSchema, test.x.primaryKeyValue)
      _ <- initializeData(test.x.table, test.dataSchema, test.x.primaryKeyValue)
      t1 = buildWriteSkewTransaction(test.x, test.y, test.dataSchema)
      t2 = buildWriteSkewTransaction(test.y, test.x, test.dataSchema)
      _ <- t1
      _ <- t2
      x <- getValue(test.x.table, test.dataSchema, test.x.resultField, test.x.primaryKeyValue)
      y <- getValue(test.y.table, test.dataSchema, test.y.resultField, test.y.primaryKeyValue)
    } yield {
      println(s"result: x = $x, y = $y")
      if (x == 0 && y == 0) {
        Result(IsolationLevels.SnapshotIsolation)
      } else {
        Result(IsolationLevels.Serializable)
      }
    }
  }

  private def buildWriteSkewTransaction(myColumn: TestColumn, otherColumn: TestColumn, schema: DataSchema): Future[Unit] = {
    sqlLikeClient.execute(transaction()
      .add(update(myColumn.table)
        .withValues(Seq((myColumn.field, 1)))
        .withCondition(s"${schema.primaryKey} = ${myColumn.primaryKeyValue}"))
      .add(update(otherColumn.table)
        .withValues(Seq((otherColumn.resultField, otherColumn.field)))
        .withCondition(s"${schema.primaryKey} = ${otherColumn.primaryKeyValue}")))
  }

  private def buildDirtyWriteTransaction(value: Int): Future[Unit] = {
    sqlLikeClient.execute(
      transaction()
        .setIsolationLevel(IsolationLevels.Serializable)
        .add(update(TestTableName0).withValues(Seq(("x", value))).withCondition(s"$PrimaryKeyField = 0"))
        .add(update(TestTableName0).withValues(Seq(("y" , value))).withCondition(s"$PrimaryKeyField = 0")))
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

  private def updateData(): Future[Unit] = {
    for {
      _ <- sqlLikeClient.execute(update(TestTableName0).withValues(Seq(("x", 0), ("y", 0))).withCondition(s"$PrimaryKeyField = 0"))
      _ <- sqlLikeClient.execute(update(TestTableName1).withValues(Seq(("x", 1), ("y", 0))).withCondition(s"$PrimaryKeyField = 0"))
    } yield {}
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