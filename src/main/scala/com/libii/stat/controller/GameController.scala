package com.libii.stat.controller

import java.text.SimpleDateFormat
import java.util.Date

import com.libii.stat.bean.{AdLog2, IndeH5Log}
import com.libii.stat.service.{GameAuService, GameNuService, GameRetainService}
import com.libii.stat.util.JdbcUtil
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object GameController {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    // 初始化环境变量
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkSession: SparkSession = SparkSession.builder()
      .config(new SparkConf().setAppName("scala-stat")
//                      .setMaster("local[*]")
      )
      .config("hive.exec.dynamic.partition", true) // 支持 Hive 动态分区
      .config("hive.exec.dynamic.partition.mode", "nonstrict") // 非严格模式
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic") // 只覆盖对应分区的数据
      .config("apache.spark.debug.maxToStringFields", 5000) // 解决Truncated the string representation of a plan since it was too large
      .config("spark.scheduler.listenerbus.eventqueue.capacity", 100000)
      .enableHiveSupport().getOrCreate()

    val ssc: SparkContext = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://statisticservice")
    ssc.hadoopConfiguration.set("dfs.nameservices", "statisticservice")
    // 开启动态分区
    //        HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
    //        HiveUtil.openCompression(sparkSession)

    // 分析 20210801 -- 20210831
    //    args(0) = "0"
    for (i <- 0 to args(0).toInt) {
      var dateStr2: String = 20210901 + i + ""
      //      dateStr2 = "20210905"
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      val dateTime: Date = sdf.parse(dateStr2)
      sdf = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
      val dateStr = sdf.format(dateTime)
      println(dateStr)

      val df: DataFrame = sparkSession.read.parquet("/input/log_entry/inde_h5_event_pre/" + dateStr)
      import sparkSession.implicits._ //隐式转换

      val h5LogDs: Dataset[IndeH5Log] = df.as[AdLog2]
        .mapPartitions(partition => {
          partition.map(data => {
            // val format = new SimpleDateFormat("yyyymmdd") // 多次创建效率低，放在外面有线程安全问题
            // val dateStr: String = format.format(new Date(data.timestamp))
            //          val dt_Format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
            //          val dateStr = dt_Format.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(data.timestamp), ZoneId.systemDefault))
            var groupId = data.groupId
            if (null == groupId) {
              groupId = 0
            }
            IndeH5Log(data.udid, data.channel, data.appId, data.timestamp, data.deviceType, data.actionType,
              data.version, data.country, groupId.toLong, data.userType, data.level, "", data.sceneId, dateStr2.toInt)
          })
        })

      h5LogDs.persist(StorageLevel.MEMORY_AND_DISK_SER)
      // 获取Jdbc参数
      val props = JdbcUtil.getJdbcProps()
      // 日新增
      GameNuService.doDnuCount(sparkSession, props, h5LogDs, dateStr2)
      // 日活跃
      val dauDS: Dataset[IndeH5Log] = GameAuService.doDauCount(sparkSession, props, h5LogDs, dateStr2)
      // 周活跃
      GameAuService.doWauCount(sparkSession, props, dateStr2)
      // 月活跃
      GameAuService.doMauCount(sparkSession, props, dateStr2)

      GameRetainService.doRetainCount(sparkSession, dauDS, props, dateStr2)
      h5LogDs.unpersist()
    }
    sparkSession.close()
    val endTime = System.currentTimeMillis()
    println("总共耗时：" + (endTime - startTime) / 1000)
  }

}
