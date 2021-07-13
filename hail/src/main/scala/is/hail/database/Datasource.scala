package is.hail.database

import scala.util.Properties
import com.zaxxer.hikari.{HikariConfig,HikariDataSource}


object Datasource {
    private val driver = Properties.envOrElse("DB_DRIVER", "org.postgresql.Driver")
    private val url = Properties.envOrElse("DB_URL", "jdbc:postgresql://vm-danner.dhclab.i.hpi.de:5432/postgres")
    private val username  = Properties.envOrElse("DB_USER", "postgres")
    private val password = Properties.envOrElse("DB_PASSWORD", "postgres")

    private val config = new HikariConfig()
    config.setJdbcUrl(url)
    config.setUsername(username)
    config.setPassword(password)
    config.setMaximumPoolSize(6)
    config.setDriverClassName(driver)
    config.setAutoCommit(false)
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "1000");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    config.addDataSourceProperty("reWriteBatchedInserts", "true")
    val datasource = new HikariDataSource(config)
}