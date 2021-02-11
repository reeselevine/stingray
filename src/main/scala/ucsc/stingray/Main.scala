package ucsc.stingray

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {
    println("Starting test")
    val yugabyteClient = YugabyteClient()
    val stingrayApp = YugabyteStingrayApp(yugabyteClient)
    val stingrayDriver = StingrayDriver(stingrayApp)
    Await.result(stingrayDriver.execute(), Duration.Inf)
    println("Test finished")
  }
}
