package com.libii.stat.service

import java.util.Properties

import com.libii.stat.bean.IndeH5Log
import com.libii.stat.util.date.DateUtils
import com.libii.stat.util.{Constant, JdbcUtil, Utils}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object GameAuService {

  /**
   * 日活
   * @param sparkSession
   * @param props
   * @param h5LogDs
   * @param dateStr
   */
  def doDauCount(sparkSession: SparkSession, props: Properties, h5LogDs: Dataset[IndeH5Log], dateStr: String) = {
    val distinctLog = h5LogDs.filter(log => log.actionType.equals(Constant.INSTALL) || log.actionType.equals(Constant.ACTIVE))
      // 日活去重，同一天同一用户同一款游戏多个日志，只保留一个
      .dropDuplicates("udid", "appId", "date")

    distinctLog.createOrReplaceTempView("active")

    val result: DataFrame = sparkSession.sql(
      """
        | select channel, appId as app_id, date, deviceType as device_type,
        | country, version, groupId as group_id, userType as user_type, count(*) num
        | from active
        | group by appId, channel, date, deviceType, country, version, groupId, userType
        |""".stripMargin)

    // 去重后的日活保存到hive表
    val hiveResult = distinctLog
      .withColumn("year", distinctLog("date").substr(0,4))
      .withColumn("month", distinctLog("date").substr(5,2))
      .withColumn("day", distinctLog("date").substr(7, 2))
    hiveResult.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.inde_h5_dau")

    // 先删除mysql已存在的数据
    JdbcUtil.executeUpdate("delete from " + JdbcUtil.INDE_H5_DAU + " where date = " + dateStr)
    // 保存到mysql
    result.write
      .mode(SaveMode.Append)
//      .option("truncate", true) // truncate = true + 使用overwrite模式 就会清空表数据，但是不会修改表结构
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_DAU, props)

  }

  /**
   * 周活
   * @param sparkSession
   * @param props
   * @param dateStr
   */
  def doWauCount(sparkSession: SparkSession, props: Properties, dateStr: String): Unit = {

    val mondayStr: String = Utils.initWeekDate(dateStr)
    import sparkSession.implicits._
    val weekLogDF: Dataset[Row] = sparkSession.sql(s"select * from dwd.inde_h5_dau where timestamp >= ${Constant.hisDateWeekStartLong}" +
      s" and timestamp <= ${Constant.hisDateWeekEndLong}")
//      .as[IndeH5Log]
//      .map(data => {
//        data.date += 0
//        data.date
//        data
//      })
      .dropDuplicates("udid", "appId", "date")

    weekLogDF.createOrReplaceTempView("activeWau")

    val result: DataFrame = sparkSession.sql(
      s"""
        | select channel, appId as app_id, $mondayStr as date, deviceType as device_type,
        | country, version, groupId as group_id, userType as user_type, count(*) num
        | from activeWau
        | group by appId, channel, date, deviceType, country, version, groupId, userType
        |""".stripMargin)

    // 先删除mysql已存在的数据
    JdbcUtil.executeUpdate("delete from " + JdbcUtil.INDE_H5_WAU + " where date = " + mondayStr)
    // 保存到mysql
    result.write
      .mode(SaveMode.Append)
//            .option("truncate", true) // truncate = true + 使用overwrite模式 就会清空表数据，但是不会修改表结构
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_WAU, props)

  }

  /**
   * 月活
   * @param sparkSession
   * @param props
   * @param dateStr
   */
  def doMauCount(sparkSession: SparkSession, props: Properties, dateStr: String): Unit = {

    val firstMonthDayStr = Utils.initMonthDate(dateStr)
    val weekLogDF: Dataset[Row] = sparkSession.sql(s"select * from dwd.inde_h5_dau where timestamp >= ${Constant.hisDateMonthStartLong}" +
      s" and timestamp <= ${Constant.hisDateMonthEndLong}")
      .dropDuplicates("udid", "appId", "year", "month")

    weekLogDF.createOrReplaceTempView("activeMau")

    val result: DataFrame = sparkSession.sql(
      s"""
         | select channel, appId as app_id, $firstMonthDayStr as date, deviceType as device_type,
         | country, version, groupId as group_id, userType as user_type, count(*) num
         | from activeMau
         | group by appId, channel, date, deviceType, country, version, groupId, userType
         |""".stripMargin)

    // 先删除mysql已存在的数据
    JdbcUtil.executeUpdate("delete from " + JdbcUtil.INDE_H5_MAU + " where date = " + firstMonthDayStr)
    // 保存到mysql
    result.write
      .mode(SaveMode.Append)
      .option("truncate", true) // truncate = true + 使用overwrite模式 就会清空表数据，但是不会修改表结构
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_MAU, props)

  }

}
