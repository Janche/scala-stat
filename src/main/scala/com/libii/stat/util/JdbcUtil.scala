package com.libii.stat.util

import java.util.Properties

object JdbcUtil {

  val DATABASE_ADDRESS = "jdbc:mysql://192.168.0.23:3306/libii-data-statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  val INDE_H5_DAU = "inde_h5_dau_scala"

  val INDE_H5_DNU = "inde_h5_dnu_scala"

  def getJdbcProps(): Properties ={
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "xuejinyu")
    props
  }

}
