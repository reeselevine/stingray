package ucsc.stingray

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session

object Main {

  def main(args: Array[String]): Unit = {
    println("hello world");
    try {
      // Create a Cassandra client.
      val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
      val session = cluster.connect

      // Create keyspace 'ybdemo' if it does not exist.
      val createKeyspace = "CREATE KEYSPACE IF NOT EXISTS ybdemo;";
      val createKeyspaceResult = session.execute(createKeyspace);
      println("Created keyspace ybdemo");

      // Create table 'employee' if it does not exist.
      val createTable = """
        CREATE TABLE IF NOT EXISTS ybdemo.employee (
          id int PRIMARY KEY,
          name varchar,
          age int,
          language varchar
        );
      """
      val createResult = session.execute(createTable)
      println("Created table employee")

      // Insert a row.
      val insert = """
        INSERT INTO ybdemo.employee
                     |(id, name, age, language)
                     |VALUES
                     |(1, 'John', 35, 'Scala');
        """.trim.stripMargin('|').replaceAll("\n", " ")
      val insertResult = session.execute(insert)
      println(s"Inserted data: ${insert}")

      // Query the row and print out the result.
      val select = """
        SELECT name, age, language
         FROM  ybdemo.employee
         WHERE id = 1;
      """
      val rows = session.execute(select).all
      val name = rows.get(0).getString(0)
      val age = rows.get(0).getInt(1)
      val language = rows.get(0).getString(2)
      println(
        s"""
          Query returned ${rows.size}
           |row: name=${name},
           |age=${age},
           |language=${language}
        """.trim.stripMargin('|').replaceAll("\n", " ")
      )

      // Close the client.
      session.close
      cluster.close
    } catch {
      case e: Throwable => println(e.getMessage)
    }
  }
}
