package com.libii.stat.util

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Properties, TimeZone}

object Constant extends Serializable {
  val dtFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val sdFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  var CHINA_TIME_ZONE: TimeZone = TimeZone.getTimeZone("GMT+8:00")
  val dateFormatTedious = new SimpleDateFormat("'year='yyyy/'month='M/'day='d")
  val dateFormatBrief = new SimpleDateFormat("yyyy/MM/dd")
  val RATE = "rate"
  val RATES = "rates"
  val CURRENCY_CODE: String = "currency_code"
  val HIS_DATA: String = "hisdata"
  val APP_INCREASED: String = "app_increased"
  val INDE_H5_INCREASED: String = "inde_h5_increased"
  val BOX_H5_INCREASED: String = "box_h5_increased"
  val APP_ACTIVE: String = "app_active"
  val INDE_H5_ACTIVE: String = "inde_h5_active"
  val LOG_REGULAR: String = "/*.log"
  val APP_RETAIN_NEW: String = "app_retain_new"
  val INDE_H5_RETAIN: String = "inde_h5_retain"
  val BOX_H5_RETAIN: String = "box_h5_retain"
  val APP_DAU_NEW: String = "app_dau_new"
  val INDE_H5_DAU: String = "inde_h5_dau"
  val APP_WAU: String = "app_wau"
  val INDE_H5_WAU: String = "inde_h5_wau"
  val APP_DNU_NEW: String = "app_dnu_new"
  val INDE_H5_DNU: String = "inde_h5_dnu"
  val APP_LAUNCH_NEW: String = "app_launch_new"
  val INDE_H5_LAUNCH: String = "inde_h5_launch"
  val APP_SESSION_NEW: String = "app_session_new"
  val INDE_H5_TIME_SEC: String = "inde_h5_time_sec"
  val BOX_H5_TIME_SEC: String = "box_h5_time_sec"
  val INDE_H5_TIME: String = "inde_h5_time"
  val BOX_H5_TIME: String = "box_h5_time"
  val OUTER_AD_TABLE_NAME: String = "outer_ad_count"
  val CONSUME_TIME: String = "consume_time"
  val APP_PURCH: String = "app_purch"
  val DATE: String = "date"
  val INSTALL: String = "install"
  val ACTIVE: String = "active"
  val LAUNCH: String = "launch"
  val GET: String = "get"
  val BUFFER: String = "buffer"
  val SHOW: String = "show"
  val CLICK: String = "click"
  val NEW: String = "new"
  val OLD: String = "old"
  val SESSION_IN: String = "sessionIn"
  val SESSION_ONT: String = "sessionOut"
  val LOG_ENTRY: String = "/log_entry"
  val EVENT_PRE: String = "event_pre"
  val EVENT_CUSTOM: String = "event_custom"
  val BOX_H5_EVENT_PRE: String = "box_h5_event_pre"
  val INDE_H5_EVENT_PRE: String = "inde_h5_event_pre"
  val PURCHASE: String = "purchase"
  val REWARDED_VIDEO: String = "rewardedVideo" //????????????

  val INTERSTITIAL: String = "interstitial" //????????????

  val BANNER: String = "banner" //????????????

  val DEVICE_TYPE: String = "device_type"
  val APP_ID: String = "app_id"
  val CHANNEL: String = "channel"
  val COUNTRY: String = "country"
  val DNU: String = "dnu"
  val VERSION: String = "version"
  val REMAIN_NUM: String = "remainNum"
  val PURCH_NUM: String = "purch_num"
  val PURCH_COUNT: String = "purch_count"
  val DAY_NUM: String = "dayNum"
  val YYYYMMDD: String = "yyyyMMdd"
  val GROUP_ID: String = "group_id"
  val NUM: String = "num"
  val APP_MAU: String = "app_mau"
  val INDE_H5_MAU: String = "inde_h5_mau"
  val INDE_H5_SCENE: String = "inde_h5_scene"
  val CUSTOM_DOT: String = "customDot"
  val GAME_LEVEL_START: String = "gameLevelStart"
  val GAME_LEVEL_PASS: String = "gameLevelPass"
  val GAME_LEVEL_FAILED: String = "gameLevelFailed"
  val INDE_H5_GAME_LEVEL: String = "inde_h5_game_level"
  val BOX_H5_GAME_LEVEL: String = "box_h5_game_level"
  val BOX_H5_BASIC_DATA: String = "box_h5_basic_data"
  val INDE_H5_CUS_DOT: String = "inde_h5_cus_dot"
  val BOX_H5_CUS_DOT: String = "box_h5_cus_dot"
  val VALID_INSTALL: String = "valid_install"
  val VALID_ACTIVE: String = "valid_active"
  val EXPOSE: String = "expose"
  val MODULE_ID: String = "moduleId"
  val PROMO: String = "promo"
  val BOX_H5: String = "box_h5"
  val INDE_H5: String = "inde_h5"
  val INDE_H5_AD: String = "inde_h5_ad"
  val BOX_H5_AD: String = "box_h5_ad"
  val INDE_H5_DAT: String = "inde_h5_dat"
  var logRootAddress: String = _ // ?????????????????????

  var isLocal: Boolean = false
  var databaseAddress: String = _
  var databaseUsername: String = _
  var databasePassword: String = _
  var connectionProperties: Properties = _
  var hisDateTediousStr: String = _ // ??????????????????????????????"'year='yyyy/'month='M/'day='d"

  var hisDateBriefStr: String = _ // ??????????????????????????????"yyyy/MM/dd"

  var hisDateInt: Int = 0 // ?????????????????????

  var hisDateWeekStartStr: String = _ // ????????????????????????????????????????????????"yyyyMMdd"
  var hisDateWeekStartLong: Long = 0L // ??????????????????????????????????????????
  var hisDateWeekEndStr: String = _   // ????????????????????????????????????????????????"yyyyMMdd"
  var hisDateWeekEndLong: Long = 0L

  var hisDateMonthStartStr: String = _ // ???????????????????????????????????????????????????"yyyyMMdd"
  var hisDateMonthStartLong: Long = 0L // ???????????????????????????????????????????????????????????????"yyyy/MM/dd"
  var hisDateMonthEndStr: String = _   // ??????????????????????????????????????????????????????"yyyyMMdd"
  var hisDateMonthEndLong: Long = 0L
}
