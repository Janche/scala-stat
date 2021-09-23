package com.libii.stat.util

import java.io.FileNotFoundException
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

import org.apache
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcUtil {

  val DATABASE_ADDRESS = "jdbc:mysql://192.168.0.23:3306/libii-data-statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  val DATABASE_USER = "root"

  val DATABASE_PASSWORD = "xuejinyu"

  val INDE_H5_DAU = "inde_h5_dau_scala"

  val INDE_H5_DNU = "inde_h5_dnu_scala"

  def getJdbcProps(): Properties = {
    val props = new Properties()
    props.setProperty("user", DATABASE_USER)
    props.setProperty("password", DATABASE_PASSWORD)
    props
  }

  // 获取mysql连接
  def getConnection(): Connection = {
    DriverManager.getConnection(DATABASE_ADDRESS, DATABASE_USER, DATABASE_PASSWORD)
  }

  // 执行mysql 新增、修改和删除
  def executeUpdate(sql: String) = {
    val conn = getConnection()
    val stmt: Statement = conn.createStatement()
    try{
      stmt.executeUpdate(sql)
    } catch {
      case e: Exception => {
        println("mysql 操作异常")
      }
    }finally {
      stmt.close()
      conn.close()
    }
  }

  // 执行mysql 查询
  def executeQuery(spark: SparkSession, tableName: String): DataFrame = {
    val df = spark.read.jdbc(DATABASE_ADDRESS, tableName, getJdbcProps())
//    val df = spark.read.jdbc(DATABASE_ADDRESS, "t_score", "id", lowerBound, upperBound, numPartitions, prop) // 指定分区的列和分区数量
    df
  }

}
