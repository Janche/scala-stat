package com.libii.stat.util

import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData, Statement, Timestamp}
import java.util.Properties
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    try {
      stmt.executeUpdate(sql)
    } catch {
      case e: Exception => {
        println("mysql 操作异常")
      }
    } finally {
      stmt.close()
      conn.close()
    }
  }

  // 执行mysql 查询
  def executeQuery(spark: SparkSession, tableName: String): DataFrame = {
    val df = spark.read
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(DATABASE_ADDRESS, tableName, getJdbcProps())
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
    val df = spark.read
      .option("driver", "com.mysql.jdbc.Driver")
      .jdbc(DATABASE_ADDRESS, INDE_H5_DNU, "date", lowerBound, upperBound, numPartitions, getJdbcProps())

    println(df.count())
    println(df.rdd.partitions.foreach(println(_)))
    df.show()
  }

  /**
   * 方式四:通过load获取,和方式二类似
   *
   * @param spark
   */
  def method4(spark: SparkSession): Unit = {
    val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .options(Map("url" -> url, "dbtable" -> "t_score")).load()

    println(df.count())
    println(df.rdd.partitions.size)
    import spark.sql
    df.createOrReplaceTempView("t_score")
    sql("select * from t_score").show()
  }

  /**
   * 方式五:加载条件查询后的数据
   *
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

  def getInsertOrUpdateSql(tableName: String, cols: Array[String], updateColumns: Array[String]): String = {
    val colNumbers = cols.length
    var sqlStr = "insert into" + tableName + "values("
    for (i <- 1 to colNumbers) {
      sqlStr += "?"
      if (i != colNumbers) {
        sqlStr += ","
      }
    }
    sqlStr += ")ON DUPLICATE KEY UPDATE"

    updateColumns.foreach(str => {
      sqlStr += s"$str =?,"
    })

    sqlStr.substring(0, sqlStr.length - 1)
  }

  /**
   * 通过insertOrUpdate的方式把DataFrame写入到MySQL中, 注意:此方式,必须对表设置主键
   *
   * @param tableName
   * @param resultDateFrame
   * @param updateColumns
   */
  def insertOrUpdateDFtoDBUsePool(tableName: String, resultDateFrame: DataFrame, updateColumns: Array[String]) {
    val colNumbers: Int = resultDateFrame.columns.length
    val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
    val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
    println("## ############ sql =" + sql)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = JdbcUtil.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaData = conn.getMetaData.getColumns(null, "％", tableName, "％") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始,record.getString()方法从0开始
          for (i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            val dateType = columnDataTypes(i - 1)
            if (value != null) { //如何值不为空,将类型转换为String
              preparedStatement.setString(i, value.toString)
              dateType match {
                case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
                case _: BooleanType => preparedStatement.setInt(i, if (record.getAs[Boolean](i - 1)) 1 else 0)
                case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
                case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
                case _: StringType => preparedStatement.setString(i, record.getAs[String](i - 1))
                case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
                case _ => new RuntimeException(s"nonsupport ${dateType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute(i)
              preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
            }

          }
          //设置需要更新的字段值
          for (i <- 1 to updateColumns.length) {
            val fieldIndex = record.fieldIndex(updateColumns(i - 1))
            val value = record.get(fieldIndex)
            val dataType = columnDataTypes(fieldIndex)
            println(s"$fieldIndex,$value,$dataType")
            if (value != null) { //如何值不为空,将类型转换为String
              preparedStatement.setString(i, value.toString)
              dataType match {
                case _: ByteType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: ShortType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: IntegerType => preparedStatement.setInt(colNumbers + i, record.getAs[Int](fieldIndex))
                case _: LongType => preparedStatement.setLong(colNumbers + i, record.getAs[Long](fieldIndex))
                case _: BooleanType => preparedStatement.setBoolean(colNumbers + i, record.getAs[Boolean](fieldIndex))
                case _: FloatType => preparedStatement.setFloat(colNumbers + i, record.getAs[Float](fieldIndex))
                case _: DoubleType => preparedStatement.setDouble(colNumbers + i, record.getAs[Double](fieldIndex))
                case _: StringType => preparedStatement.setString(colNumbers + i, record.getAs[String](fieldIndex))
                case _: TimestampType => preparedStatement.setTimestamp(colNumbers + i, record.getAs[Timestamp](fieldIndex))
                case _: DateType => preparedStatement.setDate(colNumbers + i, record.getAs[Date](fieldIndex))
                case _ => new RuntimeException(s"nonsupport ${dataType} !!!")
              }
            } else { //如果值为空,将值设为对应类型的空值
              metaData.absolute(colNumbers + i)
              preparedStatement.setNull(colNumbers + i, metaData.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(s"@@ insertOrUpdateDFtoDBUsePool ${e.getMessage}")
        //做一些log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

  /**
   * 查询结果并返回
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objects    参数
   */
  def executeSelectByBack(connection: Connection, sql: String, back: (ResultSet) => Unit, objects: Any*): Unit = {
    val statement: PreparedStatement = connection.prepareStatement(sql)
    //判断参数是否为空
    if (objects != null && objects.nonEmpty) {
      //将参数设置进去
      for (i <- objects.indices) {
        //参数索引从1开始
        statement.setObject(i + 1, objects(i))
      }
    }
    //执行查询
    val resultSet: ResultSet = statement.executeQuery()
    back(resultSet)
    //关闭资源
    resultSet.close()
    statement.close()
  }

  /**
   * 查询一条结果，返回map集合
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objects    参数
   * @return 返回map
   */
  def executeSelectOne(connection: Connection, sql: String, objects: Any*): mutable.HashMap[String, Any] = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    //判断参数是否为空
    if (objects != null && objects.nonEmpty) {
      //将参数设置进去
      for (i <- objects.indices) {
        //参数索引从1开始
        preparedStatement.setObject(i + 1, objects(i))
      }
    }
    //执行查询
    val resultSet: ResultSet = preparedStatement.executeQuery()
    //定义一个hashMap
    val resultMap = new mutable.HashMap[String, Any]()
    //获取列名信息
    val metaData: ResultSetMetaData = resultSet.getMetaData
    //获取一共有几列
    val rowCount: Int = metaData.getColumnCount
    if (resultSet.next()) {
      for (i <- 1 to rowCount) {
        resultMap.put(metaData.getColumnName(i), resultSet.getObject(i))
      }
    }
    //关闭资源
    resultSet.close()
    preparedStatement.close()
    //返回
    resultMap
  }

  /**
   * 返回多条结果
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objects    参数
   * @return List[Map]
   */
  def executeSelectAll(connection: Connection, sql: String, objects: Any*): ListBuffer[mutable.HashMap[String, Any]] = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    //判断参数是否为空
    if (objects != null && objects.nonEmpty) {
      //将参数设置进去
      for (i <- objects.indices) {
        //参数索引从1开始
        preparedStatement.setObject(i + 1, objects(i))
      }
    }
    //执行查询
    val resultSet: ResultSet = preparedStatement.executeQuery()
    //定义一个hashMap
    val resultList = new ListBuffer[mutable.HashMap[String, Any]]()
    //获取列名信息
    val metaData: ResultSetMetaData = resultSet.getMetaData
    //获取一共有几列
    val rowCount: Int = metaData.getColumnCount
    while (resultSet.next()) {
      val hashMap = new mutable.HashMap[String, Any]()
      for (i <- 1 to rowCount) {
        hashMap.put(metaData.getColumnName(i), resultSet.getObject(i))
      }
      resultList.append(hashMap)
    }
    //关闭资源
    resultSet.close()
    preparedStatement.close()
    //返回
    resultList
  }

  /**
   * 添加或者修改信息
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objects    参数
   */
  def executeUpdateOne(connection: Connection, sql: String, objects: Any*): Int = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    //判断参数是否为空
    if (objects != null && objects.nonEmpty) {
      //将参数设置进去
      for (i <- objects.indices) {
        //参数索引从1开始
        preparedStatement.setObject(i + 1, objects(i))
      }
    }
    //执行
    val resultCount: Int = preparedStatement.executeUpdate()
    //关闭
    preparedStatement.close()
    //返回
    resultCount
  }

  /**
   * 批量添加或者修改信息
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objectList 参数数组
   */
  def executeUpdateAll(connection: Connection, sql: String, objectList: List[Array[Any]]): Array[Int] = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
    //判断参数是否为空
    if (objectList != null && objectList.nonEmpty) {
      //将参数设置进去
      objectList.foreach(arr => {
        for (i <- arr.indices) {
          //参数索引从1开始
          preparedStatement.setObject(i + 1, arr(i))
        }
        preparedStatement.addBatch()
      })
    }
    //执行
    val resultCountList: Array[Int] = preparedStatement.executeBatch()
    //关闭
    preparedStatement.close()
    //返回
    resultCountList
  }

  /**
   * 添加信息并返回主键
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objects    参数
   */
  def executeInsertOne(connection: Connection, sql: String, objects: Any*): Option[Long] = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    //判断参数是否为空
    if (objects != null && objects.nonEmpty) {
      //将参数设置进去
      for (i <- objects.indices) {
        //参数索引从1开始
        preparedStatement.setObject(i + 1, objects(i))
      }
    }
    //执行
    preparedStatement.executeUpdate()
    //获取自增结果集
    val generatedKeys: ResultSet = preparedStatement.getGeneratedKeys
    var keyNum: Option[Long] = None
    //判断是否有数据
    if (generatedKeys.next()) {
      keyNum = Some(generatedKeys.getLong(1))
    }
    //关闭
    generatedKeys.close()
    preparedStatement.close()
    //返回
    keyNum
  }

  /**
   * 批量添加信息,并返回主键
   *
   * @param connection 连接对象
   * @param sql        sql语句
   * @param objectList 参数数组
   */
  def executeInsertAll(connection: Connection, sql: String, objectList: List[Array[Any]]): ListBuffer[Long] = {
    //设置sql语句
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
    //判断参数是否为空
    if (objectList != null && objectList.nonEmpty) {
      //将参数设置进去
      objectList.foreach(arr => {
        for (i <- arr.indices) {
          //参数索引从1开始
          preparedStatement.setObject(i + 1, arr(i))
        }
        preparedStatement.addBatch()
      })
    }
    //执行
    preparedStatement.executeBatch()
    //获取自增结果集
    val generatedKeys: ResultSet = preparedStatement.getGeneratedKeys
    var keyNumList = new ListBuffer[Long]()
    //判断是否有数据
    while (generatedKeys.next()) {
      keyNumList.append(generatedKeys.getLong(1))
    }
    //关闭
    generatedKeys.close()
    preparedStatement.close()
    //返回
    keyNumList
  }


}
