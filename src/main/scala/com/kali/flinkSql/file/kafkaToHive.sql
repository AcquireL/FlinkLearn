
# 1.建表语句

## （1）kafka 
{"id":12, "name":"lwj", "password":"wong", "age":22, "ts":1603769073}


CREATE TABLE stu_kafka(
  id INT,
  name STRING,
  password STRING,
  age INT,
  ts STRING,
  eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(cast(ts AS BIGINT) / 1000, 'yyyy-MM-dd HH:mm:ss')), -- 事件时间
  WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECOND -- 水印
) WITH (
  'connector.type' = 'kafka',
  'connector.version' = 'universal', -- 指定Kafka连接器版本，不能为2.4.0，必须为universal，否则会报错
  'connector.topic' = 'student', -- 指定消费的topic
  'connector.startup-mode' = 'latest-offset', -- 指定起始offset位置
  'connector.properties.zookeeper.connect' = 'acquirel:2181',
  'connector.properties.bootstrap.servers' = 'acquirel:9092',
  'connector.properties.group.id' = 'student_1',
  'format.type' = 'json',
  'format.derive-schema' = 'true', -- 由表schema自动推导解析JSON
  'update-mode' = 'append'
);

## （2）mysql

CREATE TABLE stu_mysql (
  id INT,
  name STRING,
  password STRING,
  age INT,
  ts STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://learn:3306/test',
    'connector.table' = 'stu_mysql',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = '713181',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);

CREATE TABLE stu_mysql (
  id INT,
  name varchar(255),
  password varchar(255),
  age INT,
  ts varchar(255),
  PRIMARY KEY (id) 
);


## (3) hbase

CREATE TABLE stu_hbase (
 rowkey INT,
 cf ROW<id INT,name STRING,password STRING,age INT,ts STRING>,
 PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
 'connector' = 'hbase-2.2',
 'table-name' = 'learn:stu_hbase',
 'zookeeper.quorum' = 'learn:2181'
);

INSERT INTO hTable
SELECT rowkey, ROW(f1q1), ROW(f2q2, f2q3), ROW(f3q4, f3q5, f3q6) FROM T;

-- scan data from the HBase table
SELECT rowkey, cf FROM stu_hbase;

create 'learn:stu_hbase','cf'





