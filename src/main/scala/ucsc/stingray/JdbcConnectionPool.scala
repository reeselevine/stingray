package ucsc.stingray

import java.sql._
import org.apache.commons.dbcp2._

class JdbcConnectionPool {

  val connectionPool = new BasicDataSource()
  connectionPool.setDriverClassName("org.postgresql.Driver")
  connectionPool.setUrl("jdbc:postgresql://localhost:5433/yugabyte")
  connectionPool.setUsername("yugabyte")
  connectionPool.setUsername("yugabyte")
  connectionPool.setInitialSize(2)

  def getConnection(): Connection = connectionPool.getConnection()
}

object JdbcConnectionPool {

  def apply(): JdbcConnectionPool = new JdbcConnectionPool()
}
