package ucsc.stingray

import com.datastax.driver.core.{Cluster, ResultSet}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import ucsc.stingray.StingrayApp.{DataSchema, DataTypes}
import ucsc.stingray.YugabyteClient.{DataRow, DataValue, IntDataValue, StringDataValue}

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.jdk.CollectionConverters.CollectionHasAsScala

class CqlYugabyteClient extends YugabyteClient {

  import CqlYugabyteClient._

  val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
  val session = cluster.connect

  override def execute(query: String, schemaOpt: Option[DataSchema]): Future[Seq[DataRow]] = {
    val resultSet = session.executeAsync(query).asScala
    resultSet.map { set =>
      schemaOpt match {
        case Some(schema) => set.all().asScala.map { row =>
          val data = schema.schema.foldLeft(Map[String, DataValue]()){
            case (curMap, (key, dataType)) => dataType match {
              case DataTypes.Integer => curMap + (key -> IntDataValue(row.getInt(key)))
              case DataTypes.String => curMap + (key -> StringDataValue(row.getString(key)))
            }
          }
          DataRow(row.getInt(0), data)
        }.toSeq
        case None => Seq()
      }
    }
  }

  def close(): Unit = {
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
