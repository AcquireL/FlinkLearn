
CREATE TABLE mysql_to_hive (
  id BIGINT,
  name STRING,
  age INT,
  status STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://acquirel:3306/testDb',
    'connector.table' = 'users',
    'connector.driver' = 'com.mysql.jdbc.Driver',
    'connector.username' = 'root',
    'connector.password' = '713181',
    'connector.lookup.cache.max-rows' = '5000',
    'connector.lookup.cache.ttl' = '10min'
);