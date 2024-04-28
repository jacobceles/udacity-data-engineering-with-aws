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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1714282480078 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1714282480078")

# Script generated for node Trainer Trusted
TrainerTrusted_node1714282534023 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="step_trainer_trusted", transformation_ctx="TrainerTrusted_node1714282534023")

# Script generated for node Join
Join_node1714282600291 = Join.apply(frame1=TrainerTrusted_node1714282534023, frame2=AccelerometerTrusted_node1714282480078, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1714282600291")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1714282692500 = glueContext.getSink(path="s3://spark-glue-aws/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1714282692500")
MachineLearningCurated_node1714282692500.setCatalogInfo(catalogDatabase="spark-glue-aws-db",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1714282692500.setFormat("json")
MachineLearningCurated_node1714282692500.writeFrame(Join_node1714282600291)
job.commit()