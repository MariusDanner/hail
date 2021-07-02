package is.hail.database

import scala.util.Properties
import java.sql.{Connection,DriverManager, Statement, Types, ResultSet}

object DatabaseConnector {
    def connectToDatabase(autoCommit: Boolean): Connection = {
      val driver = Properties.envOrElse("DB_DRIVER", "org.postgresql.Driver")
      Class.forName(driver)
      val url = Properties.envOrElse("DB_URL", "jdbc:postgresql://vm-danner.dhclab.i.hpi.de:5432/postgres")

      val username  = Properties.envOrElse("DB_USER", "postgres")
      val password = Properties.envOrElse("DB_PASSWORD", "postgres")

      var connection : Connection = null
      connection = DriverManager.getConnection(url, username, password)
      connection.setAutoCommit(autoCommit)
      val usingHana = "com.sap.db.jdbc.Driver".equals(Properties.envOrElse("DB_DRIVER", ""))
      if (usingHana) {
        val stmt = connection.createStatement()
        stmt.execute("SET SCHEMA FIBERGENOMICS")
      }
      connection
  }
}