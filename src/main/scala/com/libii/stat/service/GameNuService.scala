package com.libii.stat.service

import java.util.Properties

import com.libii.stat.bean.IndeH5Log
import com.libii.stat.util.{Constant, JdbcUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object GameNuService {

  def doNuCount(sparkSession: SparkSession, props: Properties, h5LogDs: Dataset[IndeH5Log], dateStr: String) = {
    val installDs: Dataset[IndeH5Log] = h5LogDs.filter(log => log.actionType == Constant.INSTALL)
    /*
     val distinctLogDF: DataFrame = installDs.dropDuplicates("udid", "appId", "date")
     // 重命名列名
         .withColumnRenamed("appId", "app_id")
         .withColumnRenamed("deviceType", "device_type")
         .withColumnRenamed("groupId", "group_id")
    */
    // 同一天数据去重（同一天同一用户可能同时安装多款游戏，同一款游戏也可能安装多次，所以要进行去重
    // udid+appId+date，表示同一用户同一天同一款游戏多个日志，只保留一个
    val distinctLogDS: Dataset[IndeH5Log] = installDs.dropDuplicates("udid", "appId", "date")

    val year = dateStr.substring(0, 4)
    val month = Integer.parseInt(dateStr.substring(4, 6))
    val day = Integer.parseInt(dateStr.substring(6))
//    sparkSession.sql("alter table dwd.inde_h5_dnu drop partition (year="+year+", month="+month+", day="+day+")")
    // 保存到hive表
    val hiveResult = distinctLogDS
      .withColumn("year", distinctLogDS("date").substr(0,4))
      .withColumn("month", distinctLogDS("date").substr(5,2))
      .withColumn("day", distinctLogDS("date").substr(7, 2))
    hiveResult.toDF("", ).show()
//    hiveResult.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.inde_h5_dnu")


    // 聚合
    val result: DataFrame = handleDataByApi(sparkSession, distinctLogDS)
    //    val result2: DataFrame = handleDataBySql(sparkSession, installDs)
    //删除mysql 已存在的数据
//    JdbcUtil.executeUpdate("delete from " + JdbcUtil.INDE_H5_DNU + " where date = " + dateStr)
//    // 保存新数据到数据库
//    result.write
//      .mode(SaveMode.Append)
//      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_DNU, props)

  }

  def handleDataByApi(sparkSession: SparkSession, distinctLogDS: Dataset[IndeH5Log]): DataFrame = {
    // groupBy 和 select 先后顺序没有关系
    import sparkSession.implicits._
    val result: DataFrame = distinctLogDS
      .select($"channel", $"appId".alias("app_id"),
        $"date", $"deviceType".alias("device_type"),
        $"country", $"version", $"groupId".alias("group_id"))
      .groupBy("channel", "app_id", "date", "device_type", "country", "version", "group_id")
//      .groupBy("channel", "appId", "date", "deviceType", "country", "version", "groupId")
      .agg(count("channel").alias("num"))

    result
  }

  def handleDataBySql(sparkSession: SparkSession, distinctLogDS: Dataset[IndeH5Log]): DataFrame = {
    distinctLogDS.createOrReplaceTempView("dataSet")
    val result: DataFrame = sparkSession.sql(
      """
        | select
        |   channel,appId as app_id,date,deviceType as device_type,country,version, groupId as group_id,count(*) as num
        | from dataSet group by channel,appId,date,deviceType,country,version,groupId
        |""".stripMargin)
    result
  }
}
