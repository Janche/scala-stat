package com.libii.stat.controller

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date
import com.libii.stat.bean.{AdLog, IndeH5Log}
import com.libii.stat.service.{GameAuService, GameNuService}
import com.libii.stat.util.{HiveUtil, JdbcUtil}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
object GameController {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    // 初始化环境变量
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkSession: SparkSession = SparkSession.builder()
      .config(new SparkConf().setAppName("scala-stat")
              .setMaster("local[*]")
      )
//      .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
//      .config("hive.exec.dynamic.partition.mode", "nonstrict") // 非严格模式
      .config("spark.sql.sources.partitionOverwriteMode","dynamic") // 只覆盖对应分区的数据
      .enableHiveSupport().getOrCreate()

    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://statisticservice")
    ssc.hadoopConfiguration.set("dfs.nameservices", "statisticservice")
    // 开启动态分区
//        HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
//        HiveUtil.openCompression(sparkSession)

    // 分析 20210801 -- 20210831
//    args(0) = "1"
    for (i <- 0 to args(0).toInt){
      var dateStr2: String = 20210801 + i + ""
//      dateStr2 = "20210807"
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val dateTime: Date = sdf.parse(dateStr2)
      sdf = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
      val dateStr = sdf.format(dateTime)
      println(dateStr)

      val df: DataFrame = sparkSession.read.parquet("/input/log_entry/inde_h5_event_pre/" + dateStr)

      import sparkSession.implicits._ //隐式转换
      val h5LogDs: Dataset[IndeH5Log] = df.as[AdLog]  // 此处需要隐式转换
      .mapPartitions(partition => {
        partition.map(data => {
          // val format = new SimpleDateFormat("yyyymmdd") // 多次创建效率低，放在外面有线程安全问题
          // val dateStr: String = format.format(new Date(data.timestamp))
          val dt_Format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
          val dateStr = dt_Format.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
          IndeH5Log(data.udid, data.channel, data.appId, Integer.parseInt(dateStr), data.timestamp, data.deviceType, data.actionType,
            data.version, data.country, data.sessionFlag, data.groupId, data.userType, data.level, data.customDotEvent, data.sceneId)
        })
      })
     //    使用map的方式
      //      {
      //        val dateStr = dtFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
      //        IndeH5Log(data.udid, data.channel, data.appId, Integer.parseInt(dateStr), data.timestamp, data.deviceType, data.actionType,
      //          data.version, data.country, data.sessionFlag, data.groupId, data.userType, data.level, data.customDotEvent, data.sceneId)
      //      }

      h5LogDs.persist()
      // 获取Jdbc参数
      val props = JdbcUtil.getJdbcProps()
      // 日新增
      GameNuService.doDnuCount(sparkSession, props, h5LogDs, dateStr2)
      // 日活跃
      GameAuService.doDauCount(sparkSession, props, h5LogDs, dateStr2)
      // 周活跃
      GameAuService.doWauCount(sparkSession, props, dateStr2)
      // 月活跃
      GameAuService.doMauCount(sparkSession, props, dateStr2)
      h5LogDs.unpersist()
    }
    sparkSession.close()
    val endTime = System.currentTimeMillis()
    println("总共耗时：" + (endTime - startTime)/1000)
  }

}
