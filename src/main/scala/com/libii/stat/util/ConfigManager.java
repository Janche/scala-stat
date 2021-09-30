//package com.libii.stat.util;
//
//import java.io.InputStream;
//import java.util.Properties;
//
///**
// *
// * 读取配置文件工具类
// */
//public class ConfigManager {
//
//  private static Properties prop = new Properties();
//
//  static {
//    try {
//      InputStream inputStream = ConfigManager.class.getClassLoader()
//              .getResourceAsStream("/root/spark_jar/druid.properties");
//
//      prop.load(inputStream);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   * 获取配置项
//   * @param key
//   * @return
//   */
//  public static String getProperty(String key) {
//    return prop.getProperty(key);
//  }
//
//  /**
//   * 获取布尔类型的配置项
//   * @param key
//   * @return
//   */
//  public static boolean getBoolean(String key) {
//    String value = prop.getProperty(key);
//    try {
//      return Boolean.valueOf(value);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    return false;
//  }
//
//}
