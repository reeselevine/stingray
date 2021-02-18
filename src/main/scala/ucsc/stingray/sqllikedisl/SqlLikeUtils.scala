package ucsc.stingray.sqllikedisl

import ucsc.stingray.StingrayApp.DataTypes

trait SqlLikeUtils {

  def buildTableSchema(
                        header: String,
                        footer: String,
                        createTableRequest: CreateTableRequest): String = {
    val primaryKeyStr = s"${createTableRequest.schema.primaryKey} int PRIMARY KEY"
    val otherFields = createTableRequest.schema.columns.map(buildFieldDefinition).toSeq
    val joinedFields = (Seq(primaryKeyStr) ++ otherFields).mkString(",")
    header + joinedFields + footer
  }

  def buildFieldDefinition(field: (String, DataTypes.Value)): String = field match {
    case (key, dataType) => dataType match {
      case DataTypes.Integer => s"$key int"
    }
  }

  def buildSelect(fullName: String, select: Select): String = {
    val columns = select.columns match {
      case Right(values) => values.mkString(",")
      case Left(_) => "*"
    }
    s"SELECT $columns FROM $fullName".withCondition(select.condition)
  }

  def buildUpsert(fullName: String, upsert: Upsert): String = upsert match {
    case insert: Insert => buildInsert(fullName, insert)
    case update: Update => buildUpdate(fullName, update)
  }

  def buildInsert(fullName: String, insert: Insert): String = {
    val fields = s"(${insert.values.map(_._1).mkString(",")})"
    val values = s"(${insert.values.map(_._2).mkString(",")})"
    s"INSERT INTO $fullName $fields VALUES $values".withCondition(insert.condition)
  }

  def buildUpdate(fullName: String, update: Update): String = {
    update.selectUpdateOpt match {
      case Some(select) =>
        val updateFields = update.values.map(_._1).mkString(",")
        val selectStr = buildSelect(select.tableName, select).replace(";", "")
        s"""
           |UPDATE $fullName SET
           |($updateFields) = ($selectStr)
           |""".stripMargin.withCondition(update.condition)
      case None =>
        val updates = update.values.map {
          case (field, value) => s"$field = $value"
        }.mkString(",")
        s"UPDATE $fullName SET $updates".withCondition(update.condition)
    }
  }

  implicit class RichString(str: String) {
    def withCondition(condition: Option[String]) = {
      condition match {
        case Some(cond) => str + " WHERE " + cond + ";"
        case None => str + ";"
      }
    }
  }

}

object SqlLikeUtils extends SqlLikeUtils