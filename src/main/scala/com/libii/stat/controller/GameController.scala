package com.libii.stat.controller

import java.text.SimpleDateFormat
import java.util.Date

import com.libii.stat.bean.IndeH5Log
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object GameController {

  var sparkSession: SparkSession = _
  var sparkConf :SparkConf = _
  var ssc: SparkContext = _

  def init(): Unit = {

    System.setProperty("HADOOP_USER_NAME", "root")
    sparkConf = new SparkConf().setAppName("scala-stat").setMaster("local[*]")
    sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://statisticservice")
    ssc.hadoopConfiguration.set("dfs.nameservices", "statisticservice")
    // 开启动态分区
//    HiveUtil.openDynamicPartition(sparkSession)
    // 开启压缩
//    HiveUtil.openCompression(sparkSession)
  }

  def main(args: Array[String]): Unit = {
    // 初始化环境
    init()
    val sdf: SimpleDateFormat = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
    val dateStr: String = sdf.format(new Date())
    println(dateStr)
    val date = "/year=2021/month=7/day=10"
    // 加载数据
    val df = sparkSession.read.parquet("/input/log_entry/inde_h5_event_pre" + date)
    val ds: DataFrame = df.toDF(Class[IndeH5Log])
    ds.foreach(println(_))
//    val df :DataFrame = sparkSession.sql("select * from log_entry.inde_h5_event_pre limit 10")
//    df.show()
//    sparkSession.close()
  }


}
