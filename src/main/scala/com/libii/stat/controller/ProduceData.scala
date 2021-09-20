package com.libii.stat.controller

import com.libii.stat.bean.AdLog
import com.libii.stat.util.Constant
import org.apache.spark.rdd.RDD

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ProduceData {
  def main(args: Array[String]): Unit = {

    val channels: Array[String] = Array("APP_STORE", "HUAWEI", "TOUTIAO", "GOOGLE_PALY")
    val appIds: Array[String] = Array("H5G35", "com.libii.crushallh5", "com.xmgame.savethegirl")
    val versions : Array[String] = Array("1.0.0", "1.1.0", "1.1.5")
    val groupIds: Array[Long] = Array(0L, 1001L)
    val deviceTypes = Array("iOS", "Android")
    val countries = Array("CN", "US", "IN")
    val cities = Array("shanghai", "chengdu")
    val userTypes = Array("new", "old")
    val actionTypes = Array("install", "active", "sessionIn", "sessionOut", "customDot")
    val timestamps = (1627747200 to 1628524800).by(86400).toArray // 2021-08-01  2021-08-10
    val levels: Array[String] = Array("1", "2")

    // 每天的条数
    val maxCount:Int = 1000
    var buffer = ArrayBuffer[AdLog]()
    val random = new Random()

    val days:Int = 10
    val formatter = DateTimeFormatter.ofPattern("'year='yyyy/'month='M/'day='d")
    val formatter2 = DateTimeFormatter.ofPattern("yyyy_MM_dd")
    for(n <- 0 to days){
      // 获取生成时间的日期目录
      val datePath = formatter.format(LocalDateTime.ofInstant(
        Instant.ofEpochMilli((1627747200 + n * 86400) * 1000L), ZoneId.systemDefault))
//      val datePathStr = formatter2.format(LocalDateTime.ofInstant(Instant.ofEpochMilli((1627747200 + n * 86400) * 1000L), ZoneId.systemDefault()))
      for (i <- 0 until maxCount){
        val udid = UUID.randomUUID().toString.substring(0, 4)
        //        val ts: Long = timestamps(random.nextInt(timestamps.length)) * 1000L
        // 获取日志的时间戳
        val ts: Long = (1627747200 + n * 86400) * 1000L
        val dateStr = formatter.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault))
        buffer += AdLog(udid, appIds(random.nextInt(appIds.length)),
          channels(random.nextInt(channels.length)),
          deviceTypes(random.nextInt(deviceTypes.length)),
          groupIds(random.nextInt(groupIds.length)),
          actionTypes(random.nextInt(actionTypes.length)),
          "",
          countries(random.nextInt(countries.length)),
          cities(random.nextInt(cities.length)),
          ts, dateStr,
          "GMT+08:00", userTypes(random.nextInt(userTypes.length)),
          "","1.0", -1, "", "105.49.10.22",
          levels(random.nextInt(levels.length)), "1.0.0",
          "v1", "v2", "v3", "v4", "v5", "v6",
          versions(random.nextInt(versions.length)))
      }

      val sparkSession = Constant.sparkSession
      val ssc = sparkSession.sparkContext
      val logStream: RDD[AdLog] = ssc.parallelize(buffer)
      import sparkSession.implicits._
      logStream.toDS().coalesce(1).write.parquet("/input/log_entry/inde_h5_event_pre/" + datePath)
    }
  }
}
