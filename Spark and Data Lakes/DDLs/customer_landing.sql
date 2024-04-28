CREATE EXTERNAL TABLE IF NOT EXISTS `spark-glue-aws-db`.`customer_landing` (
  `customerName` string,
  `email` string,
  `phone` string,
  `birthday` string,
  `serialNumber` string,
  `registrationDate` bigint,
  `lastUpdateDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `shareWithPublicAsOfDate` bigint,
  `shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('serialization.format'='1')
LOCATION 's3://spark-glue-aws/customer/landing/'
TBLPROPERTIES ('has_encrypted_data' = 'false');