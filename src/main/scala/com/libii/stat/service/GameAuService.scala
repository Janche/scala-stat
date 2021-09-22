package com.libii.stat.service

import java.util.Properties

import com.libii.stat.bean.IndeH5Log
import com.libii.stat.util.{Constant, JdbcUtil}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object GameAuService {

  def doAuCount(sparkSession: SparkSession, props: Properties, h5LogDs: Dataset[IndeH5Log]) = {
    val distinctLog = h5LogDs.filter(log => log.actionType == Constant.INSTALL || log.actionType == Constant.ACTIVE)
      // 日活去重，同一天同一用户同一款游戏多个日志，只保留一个
      .dropDuplicates("udid", "appId", "date")

    distinctLog.createOrReplaceTempView("active")
    val result: DataFrame = Constant.sparkSession.sql(
      """
        | select channel, appId as app_id, date, deviceType as device_type,
        | country, version, groupId as group_id, userType as user_type, count(*) num
        | from active
        | group by appId, channel, date, deviceType, country, version, groupId, userType
        |""".stripMargin)
    result.write
      .mode(SaveMode.Append)
      .jdbc(JdbcUtil.DATABASE_ADDRESS, JdbcUtil.INDE_H5_DAU, props)

  }

}
