package util

import java.io.InputStream
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Druid 连接池工具类
 *
 * create by LiuJinHe 2020/8/5
 */
object DruidUtils {
  private val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  // 初始化线程池
  var dataSource: DataSource = _

  val config: Config = ConfigFactory.load()

  def getString(path: String): String = {
    config.getString(path)
  }

  def getInteger(path: String): Integer = {
    config.getString(path).toInt
  }

  def getDouble(path: String): Double = {
    config.getString(path).toDouble
  }

  def main(args: Array[String]): Unit = {
    initDataSource()
    val conn = DruidUtils.getConnection
    val df: PreparedStatement = conn.prepareStatement(s"select * from inde_h5_dnu limit 3")
    val set: ResultSet = df.getResultSet
    println(set.getInt("id"))
  }

  def initDataSource(): Unit = {
    if (dataSource == null) {
      try {
        val druidProp = new Properties()
        val config: InputStream = this.getClass.getResourceAsStream("druid.properties")
        druidProp.load(config)
        println(druidProp.getProperty("url"))
        dataSource = DruidDataSourceFactory.createDataSource(druidProp)
      } catch {
        case e: Exception =>
          logger.error("初始化连接池失败...", e)
      }
    }
  }

  // 连接方式
  def getConnection: Connection = {
    dataSource.getConnection()
  }

  /**
   * 提交事务
   */
  def commit(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.commit()
      } catch {
        case e: SQLException =>
          logger.error(s"提交数据失败,conn: $conn", e)
      } finally {
        close(conn)
      }
    }
  }

  /**
   * 事务回滚
   */
  def rollback(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.rollback()
      } catch {
        case e: SQLException =>
          logger.error(s"事务回滚失败,conn: $conn", e)
      } finally {
        close(conn)
      }
    }
  }

  /**
   * 关闭连接
   */
  def close(conn: Connection): Unit = {
    if (conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,conn: $conn", e)
      }
    }
  }

  /**
   * 关闭连接
   */
  def close(ps: PreparedStatement, conn: Connection): Unit = {
    if (ps != null) {
      try {
        ps.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,ps: $ps", e)
      }
    }
    close(conn)
  }

  /**
   * 关闭连接
   */
  def close(stat: Statement, conn: Connection): Unit = {
    if (stat != null) {
      try {
        stat.close()
      } catch {
        case e: SQLException =>
          logger.error(s"关闭连接失败,stat: $stat", e)
      }
    }
    close(conn)
  }
}