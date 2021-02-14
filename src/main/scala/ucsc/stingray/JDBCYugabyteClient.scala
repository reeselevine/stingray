package ucsc.stingray

import java.sql.DriverManager

class JDBCYugabyteClient {

  val conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/yugabyte", "yugabyte", "yugabyte")
  val stmt = conn.createStatement()
}
