package com.libii.stat

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MainJob {

  def init(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://mycluster")
    ssc.hadoopConfiguration.set("dfs.nameservices", "mycluster")
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    init()

  }
}
