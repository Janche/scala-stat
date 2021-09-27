package com.libii.stat.util

import java.sql.{Connection, Date, DriverManager, Statement, Timestamp}
import java.util.Properties

import org.apache.hadoop.hive.ql.parse.ReplicationSpec.KEY
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JdbcUtil {

  val DATABASE_ADDRESS = "jdbc:mysql://192.168.0.23:3306/libii-data-statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false"

  val DATABASE_USER = "root"

  val DATABASE_PASSWORD = "xuejinyu"

  val INDE_H5_DAU = "inde_h5_dau_scala"

  val INDE_H5_DNU = "inde_h5_dnu_scala"

  val INDE_H5_WAU = "inde_h5_wau_scala"

  val INDE_H5_MAU = "inde_h5_mau_scala"

  val INDE_H5_RETAIN = "inde_h5_retain_scala"

  val HIVE_INDE_H5_DNU = "dwd.inde_h5_dnu"

  def getJdbcProps(): Properties = {
    val props = new Properties()
    props.setProperty("user", DATABASE_USER)
    props.setProperty("password", DATABASE_PASSWORD)
    props
  }

  // 获取mysql连接
  def getConnection(): Connection = {
    DriverManager.getConnection(DATABASE_ADDRESS, DATABASE_USER, DATABASE_PASSWORD)
  }

  // 执行mysql 新增、修改和删除
  def executeUpdate(sql: String) = {
    val conn = getConnection()
    val stmt: Statement = conn.createStatement()
    try{
      stmt.executeUpdate(sql)
    } catch {
      case e: Exception => {
        println("mysql 操作异常")
      }
    }finally {
      stmt.close()
      conn.close()
    }
  }

  // 执行mysql 查询
  def executeQuery(spark: SparkSession, tableName: String): DataFrame = {
    val df = spark.read.jdbc(DATABASE_ADDRESS, tableName, getJdbcProps())
    df
  }

  /**
   * 方式二:指定数据库字段的范围
   * 通过lowerBound和upperBound 指定分区的范围
   * 通过columnName 指定分区的列(只支持整形)
   * 通过numPartitions 指定分区数量 (不宜过大)
   *
   * @param spark
   */
  def executeQueryByPartition(spark: SparkSession): Unit = {
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 5
    val df = spark.read.jdbc(DATABASE_ADDRESS, INDE_H5_DNU, "date", lowerBound, upperBound, numPartitions, getJdbcProps())

    println(df.count())
    println(df.rdd.partitions.foreach(println(_)))
    df.show()
  }

  /**
   * 方式四:通过load获取,和方式二类似
   * @param spark
   */
  def method4(spark: SparkSession): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val df = spark.read.format("jdbc").options(Map("url" -> url, "dbtable" -> "t_score")).load()

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("t_score")
    sql("select * from t_score").show()
  }

  /**
   * 方式五:加载条件查询后的数据
   * @param spark
   */
  def executeByCondition(spark: SparkSession, param: String): Unit = {
    executeQueryByPartition(spark)
//    val df = spark.read.format("jdbc")
//      .options(
//        Map("url" -> DATABASE_ADDRESS,
////          "driver" -> "com.mysql.jdbc.Driver",
//          "user" -> DATABASE_USER,
//          "password" -> DATABASE_PASSWORD,
//          "dbtable" -> INDE_H5_DNU)
//    ).load()
//
//    println(df.count())
//    import spark.sql
//    df.createOrReplaceTempView("mysql_dnu")
//    sql("select * from mysql_dnu").show()

  }

  def getInsertOrUpdateSql(tableName:String,cols:Array [String],updateColumns:Array [String]):String = {
    val colNumbers = cols.length
    var sqlStr ="insert into"+ tableName +"values("
    for(i <- 1 to colNumbers){
      sqlStr +="?"
      if(i != colNumbers){
        sqlStr +=","
      }
    }
    sqlStr += ")ON DUPLICATE KEY UPDATE"

    updateColumns.foreach(str => {
      sqlStr += s"$str =?,"
    })

    sqlStr.substring(0,sqlStr.length - 1)
  }
  
  /**
   * 通过insertOrUpdate的方式把DataFrame写入到MySQL中, 注意:此方式,必须对表设置主键
   * @param tableName
   * @param resultDateFrame
   * @param updateColumns
   */
  def insertOrUpdateDFtoDBUsePool(tableName:String,resultDateFrame:DataFrame ,updateColumns:Array [String]){
    val colNumbers: Int = resultDateFrame.columns.length
    val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    println("## ############ sql ="+ sql)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = JdbcUtil.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null,"％",tableName,"％")//通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false )
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始,record.getString()方法从0开始
          for(i <- 1 to colNumbers){
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if(value != null){ //如何值不为空,将类型转换为String
              preparedStatement.setString(i,value.toString)
              dateType match {
                case _:ByteType => preparedStatement. setInt(i,record.getAs[Int](i - 1))
                case _:ShortType => preparedStatement.setInt(i,record.getAs[Int](i - 1))
                case _:IntegerType => preparedStatement.setInt(i,record.getAs[Int](i - 1))
                case _ :LongType => preparedStatement.setLong(i,record.getAs[Long](i - 1))
                case _:BooleanType => preparedStatement.setInt(i,if(record.getAs[Boolean](i - 1))1 else 0)
                case _:FloatType => preparedStatement.setFloat(i,record.getAs[Float](i - 1))
                case _:DoubleType => preparedStatement.setDouble(i,record.getAs[Double](i - 1))
                case _:StringType => preparedStatement.setString(i,record.getAs[String](i - 1))
                case _:TimestampType => preparedStatement.setTimestamp(i,record.getAs[Timestamp](i - 1))
                case _:DateType => preparedStatement.setDate(i,record.getAs[Date](i - 1))
                case _ => new RuntimeException(s"nonsupport ${dateType} !!!")
              }
            } else {//如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }

          }
//          //设置需要更新的字段值
//          用于(ⅰ< - 1至updateColumns.length){
//            VAL字段索引= record.fieldIndex(updateColumns(I - 1))
//            VAL值= record.get(字段索引)
//            VAL的dataType = columnDataTypes(字段索引)
//            println(s"@@ $ fieldIndex,$ value,$ dataType")
//            if(value！= null){//如何值不为空,将类型转换为String
//              dataType match {
//                case _:ByteType => preparedStatement.setInt (colNumbers + i,record.getAs[Int](fieldIndex))
//                case _:ShortType => preparedStatement.setInt(colNumbers + i,record.getAs[Int](fieldIndex))
//                case _:IntegerType => preparedStatement.setInt(colNumbers + i,record.getAs[Int](fieldIndex))
//                case _:LongType => preparedStatement.setLong(colNumbers + i,record.getAs[Long](fieldIndex))
//                case _ :BooleanType => preparedStatement.setBoolean(colNumbers + i,record.getAs[Boolean](fieldIndex))
//                case _:FloatType => preparedStatement.setFloat(colNumbers + i,record.getAs[Float](fieldIndex))
//                case _:DoubleType => preparedStatement.setDouble(colNumbers + i,record.getAs[Double](fieldIndex))
//                case _:StringType => preparedStatement.setString(colNumbers + i,record.getAs[String](fieldIndex))
//                case _:TimestampType => preparedStatement.setTimestamp(colNumbers + i,record.getAs[Timestamp](fieldIndex))
//                case _:DateType => preparedStatement.setDate(colNumbers + i,record.getAs[Date](fieldIndex))
//                case _ =>抛出新的RuntimeException(s"nonsupport $ {dataType} !!!")
//              }
//            } else {//如果值为空,将值设为对应类型的空值
//              metaData.absolute(colNumbers + i)
//              preparedStatement.setNull( colNumbers + i,metaData.getInt("DATA_TYPE"))
//            }
//          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e:Exception => println(s"@@ insertOrUpdateDFtoDBUsePool ${e.getMessage}")
        //做一些log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }
  

}
