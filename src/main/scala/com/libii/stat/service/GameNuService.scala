package com.libii.stat.service

import com.libii.stat.bean.IndeH5Log
import com.libii.stat.util.{Constant, JdbcUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object GameNuService {

  def doDnuCount(sparkSession: SparkSession, props: Properties, h5LogDs: Dataset[IndeH5Log], dateStr: String) = {
    val installDs: Dataset[IndeH5Log] = h5LogDs.filter(log => log.actionType.equals(Constant.INSTALL))

    // 同一天数据去重（同一天同一用户可能同时安装多款游戏，同一款游戏也可能安装多次，所以要进行去重
    // udid+appId+date，表示同一用户同一天同一款游戏多个日志，只保留一个
    val distinctLogDS: Dataset[IndeH5Log] = installDs.dropDuplicates("udid", "appId", "date")
    // 保存到hive表
    distinctLogDS.coalesce(1).write.mode(SaveMode.Overwrite)
//      .partitionBy("date") // 没有创建表，可通过 partitionBY + saveAsTable 创建表结构 和 插入数据
      .insertInto("dwd.inde_h5_dnu")

    // 聚合
    val result: DataFrame = handleDataByApi(sparkSession, distinctLogDS)
    //    val result2: DataFrame = handleDataBySql(sparkSession, installDs)
    //删除mysql 已存在的数据
    JdbcUtil.executeUpdate("delete from " + JdbcUtil.INDE_H5_DNU + " where date = " + dateStr)
//    // 保存新数据到数据库
    result.write
      .mode(SaveMode.Append)
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_DNU, props)

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
