package com.libii.stat.controller

import java.text.SimpleDateFormat
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.Date

import com.libii.stat.bean.{AdLog, IndeH5Log}
import com.libii.stat.service.{GameAuService, GameNuService}
import com.libii.stat.util.{Constant, HiveUtil, JdbcUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql._

object GameController {

  def main(args: Array[String]): Unit = {

    val sdf: SimpleDateFormat = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
    val dateStr: String = sdf.format(new Date())
    println(dateStr)
//    val date = "/year=2021/month=8/day=1/data_2021_08_01.log"
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

    val h5LogDs: Dataset[IndeH5Log] = df.as[AdLog]  // 此处需要隐式转换
    .mapPartitions(partition => {
      partition.map(data => {
        // val format = new SimpleDateFormat("yyyymmdd") // 多次创建效率低，放在外面有线程安全问题
        // val dateStr: String = format.format(new Date(data.timestamp))
        val dateStr = Constant.dtFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
        IndeH5Log(data.udid, data.channel, data.appId, Integer.parseInt(dateStr), data.timestamp, data.deviceType, data.actionType,
          data.version, data.country, data.sessionFlag, data.groupId, data.userType, data.level, data.customDotEvent, data.sceneId)
      })
    })
   //    使用map的方式
    //      {
    //        val dateStr = Constant.dtFormat.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
    //        IndeH5Log(data.udid, data.channel, data.appId, Integer.parseInt(dateStr), data.timestamp, data.deviceType, data.actionType,
    //          data.version, data.country, data.sessionFlag, data.groupId, data.userType, data.level, data.customDotEvent, data.sceneId)
    //      }

    h5LogDs.persist()
    // 获取Jdbc参数
    val props = JdbcUtil.getJdbcProps()
    // 日新增
    GameNuService.doNuCount(sparkSession, props, h5LogDs)
    // 日活跃
//    GameAuService.doAuCount(sparkSession, props, h5LogDs)

    h5LogDs.unpersist()
    sparkSession.close()
  }

}
