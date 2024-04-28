import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1714259552797 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1714259552797")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1714259551459 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1714259551459")

# Script generated for node Join Customer
JoinCustomer_node1714259621965 = Join.apply(frame1=CustomerTrusted_node1714259552797, frame2=AccelerometerLanding_node1714259551459, keys1=["email"], keys2=["user"], transformation_ctx="JoinCustomer_node1714259621965")

# Script generated for node Drop Fields
DropFields_node1714259784261 = DropFields.apply(frame=JoinCustomer_node1714259621965, paths=["email", "customername", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1714259784261")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714259647974 = glueContext.getSink(path="s3://spark-glue-aws/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1714259647974")
AccelerometerTrusted_node1714259647974.setCatalogInfo(catalogDatabase="spark-glue-aws-db",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1714259647974.setFormat("json")
AccelerometerTrusted_node1714259647974.writeFrame(DropFields_node1714259784261)
job.commit()