import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1714281487636 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1714281487636")

# Script generated for node Trainer Landing
TrainerLanding_node1714286583369 = glueContext.create_dynamic_frame.from_catalog(database="spark-glue-aws-db", table_name="step_trainer_landing", transformation_ctx="TrainerLanding_node1714286583369")

# Script generated for node Join
SqlQuery378 = '''
SELECT DISTINCT step_trainer_landing.sensorreadingtime, step_trainer_landing.serialnumber, step_trainer_landing.distancefromobject FROM step_trainer_landing JOIN customer_curated ON step_trainer_landing.serialnumber=customer_curated.serialnumber; 
'''
Join_node1714287356184 = sparkSqlQuery(glueContext, query = SqlQuery378, mapping = {"customer_curated":CustomerCurated_node1714281487636, "step_trainer_landing":TrainerLanding_node1714286583369}, transformation_ctx = "Join_node1714287356184")

# Script generated for node Trainer Trusted
TrainerTrusted_node1714282303938 = glueContext.getSink(path="s3://spark-glue-aws/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrainerTrusted_node1714282303938")
TrainerTrusted_node1714282303938.setCatalogInfo(catalogDatabase="spark-glue-aws-db",catalogTableName="step_trainer_trusted")
TrainerTrusted_node1714282303938.setFormat("json")
TrainerTrusted_node1714282303938.writeFrame(Join_node1714287356184)
job.commit()