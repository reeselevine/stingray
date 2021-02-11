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
}

object YugabyteClient {

  implicit class RichListenableFuture[T](lf: ListenableFuture[T]) {
    def asScala[T]: Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        override def onFailure(t: Throwable): Unit = p.failure(t)
        override def onSuccess(result: T): Unit = p.success(result)
      })
      p.future
    }
  }
}
