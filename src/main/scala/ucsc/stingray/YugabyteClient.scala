package ucsc.stingray

import com.datastax.driver.core.{Cluster, ResultSet}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

class YugabyteClient {

  import YugabyteClient._

  val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
  val session = cluster.connect

  def execute(query: String): Future[ResultSet] = {
    session.executeAsync(query).asScala
  }

  def close(): Unit = {
    session.close()
    cluster.close()
  }
}

object YugabyteClient {

  def apply(): YugabyteClient = new YugabyteClient()

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
