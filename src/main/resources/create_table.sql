# 下面是可重复写入的表结,
# 也可以不创建表 采用 overwrite + saveAsTable,自动创建表, 然后改成 overwrite + insertInto 就可以覆盖写入分区数据了
drop table `dwd.inde_h5_dau`;create table if not exists `dwd.inde_h5_dau`(
        `udid` string,
        `channel` string,
        `appid` string,
        `date` int,
        `timestamp` bigint,
        `devicetype` string,
        `actiontype` string,
        `version` string,
        `country` string,
        `sessionflag` int,
        `groupid` bigint,
        `usertype` string,
        `level` string,
        `customdotevent` string,
        `sceneid` string)
 PARTITIONED BY (
   `year` string,
   `month` string,
   `day` string)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 WITH SERDEPROPERTIES (
   'path'='hdfs://statisticservice/warehouse/tablespace/managed/hive/dwd.db/inde_h5_dau')
 STORED AS PARQUET
 LOCATION
   'hdfs://statisticservice/warehouse/tablespace/managed/hive/dwd.db/inde_h5_dau'
 TBLPROPERTIES (
   'immutable'='false',
   'auto.purge'='true',
   'transactional'='false',
   'spark.sql.create.version'='2.4.5',
   'spark.sql.partitionProvider'='catalog',
   'spark.sql.sources.provider'='parquet',
   'spark.sql.sources.schema.numPartCols'='3',
   'spark.sql.sources.schema.numParts'='1',
   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"udid","type":"string","nullable":true,"metadata":{}},{"name":"channel","type":"string","nullable":true,"metadata":{}},{"name":"appId","type":"string","nullable":true,"metadata":{}},{"name":"date","type":"integer","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}},{"name":"deviceType","type":"string","nullable":true,"metadata":{}},{"name":"actionType","type":"string","nullable":true,"metadata":{}},{"name":"version","type":"string","nullable":true,"metadata":{}},{"name":"country","type":"string","nullable":true,"metadata":{}},{"name":"sessionFlag","type":"integer","nullable":true,"metadata":{}},{"name":"groupId","type":"long","nullable":true,"metadata":{}},{"name":"userType","type":"string","nullable":true,"metadata":{}},{"name":"level","type":"string","nullable":true,"metadata":{}},{"name":"customDotEvent","type":"string","nullable":true,"metadata":{}},{"name":"sceneId","type":"string","nullable":true,"metadata":{}},{"name":"year","type":"string","nullable":true,"metadata":{}},{"name":"month","type":"string","nullable":true,"metadata":{}},{"name":"day","type":"string","nullable":true,"metadata":{}}]}',
   'spark.sql.sources.schema.partCol.0'='year',
   'spark.sql.sources.schema.partCol.1'='month',
   'spark.sql.sources.schema.partCol.2'='day',
   'transient_lastDdlTime'='1632549012');


  CREATE TABLE `dwd.inde_h5_dnu`(
       `udid` string,
       `channel` string,
       `appid` string,
       `date` int,
       `timestamp` bigint,
       `devicetype` string,
       `actiontype` string,
       `version` string,
       `country` string,
       `sessionflag` int,
       `groupid` bigint,
       `usertype` string,
       `level` string,
       `customdotevent` string,
       `sceneid` string)
 PARTITIONED BY (
   `year` int,
   `month` int,
   `day` int)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS PARQUET
 LOCATION
   'hdfs://statisticservice/warehouse/tablespace/managed/hive/dwd.db/inde_h5_dnu'
 TBLPROPERTIES (
   'auto.purge'='true',
   'bucketing_version'='2',
   'immutable'='false',
   'parquet.compression'='SNAPPY',
   'spark.sql.create.version'='2.2 or prior',
   'spark.sql.sources.schema.numPartCols'='3',
   'spark.sql.sources.schema.numParts'='1',
   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"udid","type":"string","nullable":true,"metadata":{}},{"name":"channel","type":"string","nullable":true,"metadata":{}},{"name":"appid","type":"string","nullable":true,"metadata":{}},{"name":"date","type":"integer","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}},{"name":"devicetype","type":"string","nullable":true,"metadata":{}},{"name":"actiontype","type":"string","nullable":true,"metadata":{}},{"name":"version","type":"string","nullable":true,"metadata":{}},{"name":"country","type":"string","nullable":true,"metadata":{}},{"name":"sessionflag","type":"integer","nullable":true,"metadata":{}},{"name":"groupid","type":"long","nullable":true,"metadata":{}},{"name":"usertype","type":"string","nullable":true,"metadata":{}},{"name":"level","type":"string","nullable":true,"metadata":{}},{"name":"customdotevent","type":"string","nullable":true,"metadata":{}},{"name":"sceneid","type":"string","nullable":true,"metadata":{}},{"name":"year","type":"integer","nullable":true,"metadata":{}},{"name":"month","type":"integer","nullable":true,"metadata":{}},{"name":"day","type":"integer","nullable":true,"metadata":{}}]}',
   'spark.sql.sources.schema.partCol.0'='year',
   'spark.sql.sources.schema.partCol.1'='month',
   'spark.sql.sources.schema.partCol.2'='day',
   'transient_lastDdlTime'='1632553624')


drop table `dwd.inde_h5_dau`;create table if not exists `dwd.inde_h5_dau`(
  `udid` string,
  `channel` string,
  `appid` string,
  `timestamp` bigint,
  `devicetype` string,
  `actiontype` string,
  `version` string,
  `country` string,
  `groupid` bigint,
  `usertype` string,
  `level` string,
  `customdotevent` string,
  `sceneid` string)
 PARTITIONED BY (
   `date` int)
 ROW FORMAT SERDE
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 WITH SERDEPROPERTIES (
   'path'='hdfs://statisticservice/warehouse/tablespace/managed/hive/dwd.db/inde_h5_dau')
 STORED AS INPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
   'hdfs://statisticservice/warehouse/tablespace/managed/hive/dwd.db/inde_h5_dau'
 TBLPROPERTIES (
     'auto.purge'='true',
     'immutable'='false',
     'transactional'='false',
   'spark.sql.create.version'='2.4.5',
   'spark.sql.partitionProvider'='catalog',
   'spark.sql.sources.provider'='parquet',
   'spark.sql.sources.schema.numPartCols'='1',
   'spark.sql.sources.schema.numParts'='1',
   'spark.sql.sources.schema.part.0'='{"type":"struct","fields":[{"name":"udid","type":"string","nullable":true,"metadata":{}},{"name":"channel","type":"string","nullable":true,"metadata":{}},{"name":"appId","type":"string","nullable":true,"metadata":{}},{"name":"timestamp","type":"long","nullable":true,"metadata":{}},{"name":"deviceType","type":"string","nullable":true,"metadata":{}},{"name":"actionType","type":"string","nullable":true,"metadata":{}},{"name":"version","type":"string","nullable":true,"metadata":{}},{"name":"country","type":"string","nullable":true,"metadata":{}},{"name":"groupId","type":"long","nullable":true,"metadata":{}},{"name":"userType","type":"string","nullable":true,"metadata":{}},{"name":"level","type":"string","nullable":true,"metadata":{}},{"name":"customDotEvent","type":"string","nullable":true,"metadata":{}},{"name":"sceneId","type":"string","nullable":true,"metadata":{}},{"name":"date","type":"integer","nullable":true,"metadata":{}}]}',
   'spark.sql.sources.schema.partCol.0'='date',
   'transient_lastDdlTime'='1632663957') ;