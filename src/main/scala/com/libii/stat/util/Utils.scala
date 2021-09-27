package com.libii.stat.util

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

object Utils {

  def main(args: Array[String]): Unit = {

    println(initMonthDate("20210923"))
    println(initMonthDate("20210924"))
    println(initMonthDate("20210925"))
    println(initMonthDate("20210926"))
//    initMonthDate("20210924")
//    println(Constant.hisDateMonthStartStr, Constant.hisDateMonthEndLong)
    println(Constant.hisDateMonthStartStr, Constant.hisDateMonthEndStr)
  }

  def initMonthDate(dateStr: String): String = {
    val date: Date = Constant.sdFormat.parse(dateStr)
    val cal: Calendar = Calendar.getInstance
    cal.setTimeZone(Constant.CHINA_TIME_ZONE)
    cal.setTime(date)

    Constant.hisDateMonthStartStr = dateStr.substring(0, dateStr.length - 2) + "01"
    Constant.hisDateMonthStartLong = cal.getTimeInMillis

    cal.setTime(Constant.sdFormat.parse(Constant.hisDateMonthStartStr))
    cal.add(Calendar.MONTH, 1)
    Constant.hisDateMonthEndLong = cal.getTimeInMillis - 1
    Constant.hisDateMonthEndStr = Constant.sdFormat.format(new Date(Constant.hisDateMonthEndLong))
    Constant.hisDateMonthStartStr
  }

  def initWeekDate(dateStr: String): String = {
    val date: Date = Constant.sdFormat.parse(dateStr)
    val cal: Calendar = Calendar.getInstance
    cal.setTimeZone(Constant.CHINA_TIME_ZONE)
    cal.setTime(date)
    val week: Int = cal.get(Calendar.DAY_OF_WEEK)

    if (week == 1) { // 1为星期天
      cal.add(Calendar.DATE, -6)
    } else {
      cal.add(Calendar.DATE, -(week - 2))
    }
    Constant.hisDateWeekStartStr = Constant.sdFormat.format(cal.getTime)
    Constant.hisDateWeekStartLong = cal.getTimeInMillis

    //加7天到星期天
    cal.add(Calendar.DATE, 6)
    Constant.hisDateWeekEndStr = Constant.sdFormat.format(cal.getTime)
    Constant.hisDateWeekEndLong = cal.getTimeInMillis
    Constant.hisDateWeekStartStr
  }

  /**
   * 获取某一天的前n天的日期 要获取前一天的日期，n就为1
   * 输入格式：('yyyyMMdd') -> 输出格式（"yyyyMMdd"）
   */
  def beforeNDays(dayStr: String, n: Int): String = {
    try {
      val dateNow: Date = Constant.sdFormat.parse(dayStr)
      val cl = Calendar.getInstance
      cl.setTime(dateNow)
      cl.add(Calendar.DATE, -n) // n天

      val dateFrom = cl.getTime
      return Constant.sdFormat.format(dateFrom)
    } catch {
      case e: ParseException =>
        println(e.getMessage, e)
    }
    ""
  }
}
