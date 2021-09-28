package com.libii.stat.util

import java.io.{FileInputStream, InputStream}
import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Statement}
import java.util.Properties
import com.alibaba.druid.pool.DruidDataSourceFactory

import javax.sql.DataSource
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
    val c = ConfigFactory.load("druid.properties")
    println(c.getString("username"))

    var sql = "select * from inde_h5_dnu_scala limit 3"
    initDataSource()
    val conn = DruidUtils.getConnection
    val maps: ListBuffer[mutable.HashMap[String, Any]] = JdbcUtil.executeSelectAll(conn, sql)
    maps.foreach(println(_))
    sql = "select * from inde_h5_dnu_scala where date = ? limit 3"
    JdbcUtil.executeSelectAll(conn, sql, 20210904).foreach(println(_))

    conn.close()
  }

  def initDataSource(): Unit = {
    if (dataSource == null) {
      try {
        val stream: InputStream = this.getClass.getClassLoader.getResourceAsStream("druid.properties")
        val confPath = Thread.currentThread().getContextClassLoader.getResource("druid.properties").getPath
        val prop: Properties = new Properties()
//        prop.load(new FileInputStream(confPath))
        prop.load(stream)
        dataSource = DruidDataSourceFactory.createDataSource(prop)
      } catch {
        case e: Exception =>
          logger.error("初始化Druid连接池失败...", e)
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