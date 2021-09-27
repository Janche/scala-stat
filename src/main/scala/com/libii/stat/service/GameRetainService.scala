package com.libii.stat.service

import com.libii.stat.bean.IndeH5Log
import com.libii.stat.util.{JdbcUtil, Utils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Connection
import java.util.Properties

object GameRetainService {

  /**
   * 未完成
   * @param sparkSession
   * @param dauDF2
   * @param n
   * @param dateStr
   * @return
   */
  def getNDayRetain(sparkSession: SparkSession, dauDF2: DataFrame, n: Int, dateStr: String): DataFrame = {
    import sparkSession.implicits._
    // 获取多日的历史新增数据
    val beforeDateStr: String = Utils.beforeNDays(dateStr, n)
    val hisDnuLog = sparkSession.sql(
      s"""
         | select
         |  *
         | from ${JdbcUtil.HIVE_INDE_H5_DNU}
         | where date = $beforeDateStr
         |""".stripMargin)
      .as[IndeH5Log]
//    var columnBuffer = ListBuffer[String]()
//    columnBuffer += ("appId", "channel", "date", "deviceType", "version", "country", "groupId") //"retained_num_" + n
    var columnStr = "appId, channel, date, deviceType, version, country, groupId"
    for(i <- Array(1, 2, 3, 4, 5, 6, 7, 14, 30)){
      columnStr += ", retained_num_" + i
    }
    val retainDF: DataFrame = dauDF2.join(hisDnuLog, Seq("udid", "appId"), "inner")
      .groupBy( "appId", "channel", "date", "deviceType", "version", "country", "groupId")
      .agg(count("udid").as("retained_num_" + n))
      .select( columnStr.mkString(","))
      .withColumnRenamed("appId", "app_id")
      .withColumnRenamed("deviceType", "device_type")
      .withColumnRenamed("groupId", "group_id")
//    getNDayRetain(sparkSession, retainDF, n)
    retainDF
  }

  /**
   * 计算多日留存
   * 待优化：针对每日留存都要更新一次，优化思路：将多日留存数据 放到一个dataframe中
   * @param sparkSession
   * @param dauDF
   * @param props
   * @param dateStr
   */
  def doRetainCount(sparkSession: SparkSession, dauDF: Dataset[IndeH5Log], props: Properties, dateStr: String) = {
    val dnuMysqlDF: DataFrame = JdbcUtil.executeQuery(sparkSession, JdbcUtil.INDE_H5_DNU)
    val dauDF2: DataFrame = dauDF.select("udid", "appId")
    for (n <- Array(1, 2, 3, 4, 5, 6, 7, 14, 30)){
      import sparkSession.implicits._
      // 获取多日的历史新增数据
      val beforeDateStr: String = Utils.beforeNDays(dateStr, n)
      val hisDnuLog = sparkSession.sql(
        s"""
          | select
          |  *
          | from ${JdbcUtil.HIVE_INDE_H5_DNU}
          | where date = $beforeDateStr
          |""".stripMargin)
          .as[IndeH5Log]
      val retainDF: DataFrame = dauDF2.join(hisDnuLog, Seq("udid", "appId"), "inner")
        .groupBy( "appId", "channel", "date", "deviceType", "version", "country", "groupId")
        .agg(count("udid").as("retained_num_" + n))
        .select( "appId", "channel", "date", "deviceType", "version", "country", "groupId", "retained_num_" + n )
        .withColumnRenamed("appId", "app_id")
        .withColumnRenamed("deviceType", "device_type")
        .withColumnRenamed("groupId", "group_id")

        val result = retainDF.join(dnuMysqlDF,
          Seq("app_id", "channel", "date", "device_type", "version", "country", "group_id"), "inner")
          .withColumnRenamed("num", "dnu")
          .select("app_id", "channel", "date", "device_type", "version", "country", "group_id", "dnu", "retained_num_" + n)
      val retained_num_n = "retained_num_" + n

      result.foreachPartition(data => {
        var connection: Connection = null
        try {
          classOf[com.mysql.jdbc.Driver]
          connection = JdbcUtil.getConnection()
          while (data.hasNext) {
            val row = data.next()
            val app_id = row.getAs[String]("app_id")
            val date = row.getAs[Int]("date")
            val device_type = row.getAs[String]("device_type")
            val version = row.getAs[String]("version")
            val country = row.getAs[String]("country")
            val channel = row.getAs[String]("channel")
            val group_id = row.getAs[Long]("group_id")
            val dnu = row.getAs[Long]("dnu")
            val retained_value = row.getAs[Long](retained_num_n)
            val sql = s"insert into ${JdbcUtil.INDE_H5_RETAIN} " +
              s"( app_id, date, device_type, version, country, channel, group_id, dnu, $retained_num_n) " +
              s"values ('$app_id', '$date', '$device_type', '$version', '$country', '$channel', $group_id, $dnu, $retained_value) " +
              s"ON DUPLICATE KEY UPDATE dnu = $dnu, $retained_num_n = $retained_value"
            connection.createStatement().executeUpdate(sql)
          }
        } catch {
          case e: Exception => println(e.printStackTrace())
        } finally {
          connection.close()
        }
      })
    }
  }

}
