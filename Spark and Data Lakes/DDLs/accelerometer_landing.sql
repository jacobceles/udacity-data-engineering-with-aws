CREATE EXTERNAL TABLE IF NOT EXISTS `spark-glue-aws-db`.`accelerometer_landing` (
  `user` string,
  `timestamp` bigint,
  `x` float,
  `y` float,
  `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1')
LOCATION 's3://spark-glue-aws/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');