package com.libii.stat.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object Utils {

  def main(args: Array[String]): Unit = {

    initWeekDate("20210924")
    initMonthDate("20210924")
    println(Constant.hisDateMonthStartStr, Constant.hisDateMonthEndLong)
  }

  def initMonthDate(dateStr: String): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date: Date = sdf.parse(dateStr)
    val cal: Calendar = Calendar.getInstance
    cal.setTimeZone(Constant.CHINA_TIME_ZONE)
    cal.setTime(date)


    Constant.hisDateMonthStartStr = dateStr.substring(0, dateStr.length - 2) + "01"
    val firstMonthDayStr = Constant.hisDateMonthStartStr
    Constant.hisDateMonthStartLong = cal.getTimeInMillis

    cal.setTime(sdf.parse(firstMonthDayStr))
    cal.add(Calendar.MONTH, 1)
    Constant.hisDateMonthEndLong = cal.getTimeInMillis - 1
    Constant.hisDateMonthEndStr = sdf.format(new Date(Constant.hisDateMonthEndLong))
    firstMonthDayStr
  }

  def initWeekDate(dateStr: String): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val date: Date = sdf.parse(dateStr)
    val cal: Calendar = Calendar.getInstance
    cal.setTimeZone(Constant.CHINA_TIME_ZONE)
    cal.setTime(date)
    val week: Int = cal.get(Calendar.DAY_OF_WEEK)

    if (week == 1) { // 1为星期天
      cal.add(Calendar.DATE, -6)
    } else {
      cal.add(Calendar.DATE, -(week - 2))
    }
    Constant.hisDateWeekStartStr = Constant.dateFormatTedious.format(cal.getTime)
    Constant.hisDateWeekStartLong = cal.getTimeInMillis
    val mondayStr = sdf.format(new Date(cal.getTimeInMillis))

    //加7天到星期天
    cal.add(Calendar.DATE, 6)
    Constant.hisDateWeekEndStr = Constant.dateFormatTedious.format(cal.getTime)
    Constant.hisDateWeekEndLong = cal.getTimeInMillis
    mondayStr
  }
}
