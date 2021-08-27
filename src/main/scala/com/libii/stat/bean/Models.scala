package com.libii.stat.bean


case class BaseLog(
                    udid: String, // 设备唯一标识
                    channel: String, // 游戏渠道
                    appId: String, // 应用Bundle ID
                    date: Int,
                    timestamp: Long // 时间戳（服务器获取）
                  )

case class BaseGameLog(
                        udid: String, // 设备唯一标识
                        channel: String, // 游戏渠道
                        appId: String, // 应用Bundle ID
                        date: Int,
                        timestamp: Long, // 时间戳（服务器获取）
                        deviceType: String, // 设备类型
                        actionType: String,
                        version: String, //app版本
                        country: String, //国家
                        sessionFlag: Integer, // session标号，从0开始，无效的为-1。
                        groupId: Integer //分组id
                      )

case class IndeH5Log(
                      udid: String, // 设备唯一标识
                      channel: String, // 游戏渠道
                      appId: String, // 应用Bundle ID
                      date: Int,
                      timestamp: Long, // 时间戳（服务器获取）
                      deviceType: String, // 设备类型
                      actionType: String,
                      version: String, //app版本
                      country: String, //国家
                      sessionFlag: Integer, // session标号，从0开始，无效的为-1。
                      groupId: Integer, //分组id
                      userType: String = "", //用户类型
                      level: String, //关卡级别
                      customDotEvent: String, //自定义打点事件
                      sceneId: String //场景值ID
                    )