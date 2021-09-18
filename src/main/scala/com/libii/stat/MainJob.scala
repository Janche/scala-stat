package com.libii.stat

import org.apache
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    System.setProperty("HADOOP_USER_NAME", "root")
    val sparkConf = new SparkConf().setAppName("ads_member_controller").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._ //隐式转换
    var data: DataFrame = Seq(
      ("89", "ming", "hlj","men","2019-09-08 19:19:19"),
      ("2", "kun", "xj","women","2019-09-07 10:13:15"),
      ("105", "kun", "xz","women","2019-09-02 20:20:20"),
      ("1000", "pig", "mc","women","2019-09-05 09:09:11"),
      ("1012", "pig", "hk","women","2019-09-04 10:11:11"),
      ("12", "long", "jx","men","2019-09-08 00:11:11"),
      ("110", "long", "fj","men","2019-09-07 01:01:01"),
      ("30", "sun", "ln","men","2019-09-08 11:11:11"),
      ("1", "sun", "jl","men","2019-09-06 13:11:11"),
      ("200", "tian", "jl","women","2019-09-07 02:02:02"),
      ("4", "tian", "bj","women","2019-09-08 12:12:12"),
      ("4", "tian", "bj","women","2019-09-08 12:12:12"),
      ("50", "tian", "bj","women","2019-09-07 13:13:13")
    ).toDF("useid", "name", "live","gender","createtime")
    val oCount = data.count()
    val count = data.dropDuplicates().count()
    val count2 = data.dropDuplicates("live", "gender")
    count2.show()
    println(oCount)
    println(count)
  }
}
