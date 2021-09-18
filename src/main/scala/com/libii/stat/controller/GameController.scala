package com.libii.stat.controller

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.{Date, Properties}

import com.libii.stat.bean.{AdLog, IndeH5Log}
import com.libii.stat.util.{Constant, HiveUtil}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object GameController {


  def main(args: Array[String]): Unit = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
    val dateStr: String = sdf.format(new Date())
    println(dateStr)
    val date = "/year=2021/month=9/day=13"
    // 初始化环境变量
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkSession: SparkSession = Constant.sparkSession
    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://statisticservice")
    ssc.hadoopConfiguration.set("dfs.nameservices", "statisticservice")
    // 开启动态分区
    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    HiveUtil.openCompression(sparkSession)

    import sparkSession.implicits._ //隐式转换
    val df: DataFrame = sparkSession.read.parquet("/input/log_entry/inde_h5_event_pre" + date)

    val h5LogDs: Dataset[IndeH5Log] = df.as[AdLog]
    .map(data => {
        // val format = new SimpleDateFormat("yyyymmdd") // 多次创建效率低，放在外面有线程安全问题
        // val dateStr: String = format.format(new Date(data.timestamp))
        val dateStr = Constant.dtFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
        IndeH5Log(data.udid, data.channel, data.appId, Integer.parseInt(dateStr), data.timestamp, data.deviceType, data.actionType,
          data.version, data.country, data.sessionFlag, data.groupId, data.userType, data.level, data.customDotEvent, data.sceneId)
      })

    h5LogDs.persist() //隐式转换
    doNuCount(sparkSession, h5LogDs)

    sparkSession.close()
  }

  def doNuCount(sparkSession: SparkSession, h5LogDs: Dataset[IndeH5Log]) = {
    val installDs: Dataset[IndeH5Log] = h5LogDs.filter(log => log.actionType == Constant.INSTALL)
    val count = installDs.count()

    // 同一天数据去重（同一天同一用户可能同时安装多款游戏，同一款游戏也可能安装多次，所以要进行去重
    // udid+appId+date，表示同一用户同一天同一款游戏多个日志，只保留一个
    val distinctLog = installDs.dropDuplicates("udid", "appId", "date")
    val disCount = distinctLog.count()
    println(s"去重前：${count}, 去重后：${disCount}")

    distinctLog.createOrReplaceTempView("dataSet")
    val result: DataFrame = sparkSession.sql(
      """
        | select
        |   channel,appId as app_id,date,deviceType as device_type,country,version, groupId as group_id,count(*) as num
        | from dataSet group by channel,appId,date,deviceType,country,version,groupId
        |""".stripMargin)
    //删除旧数据
//    Utils.deleteOldResult(date, tableName, Constants.DATE)

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "xuejinyu")
    //保存新数据
    result.write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.0.23:3306/libii-data-statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "inde_h5_dnu_scala", props)

  }

}
