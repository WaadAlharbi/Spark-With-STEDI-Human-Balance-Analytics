import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node curated customers
curatedcustomers_node1739802909540 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customers_curated", transformation_ctx="curatedcustomers_node1739802909540")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1739802908765 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1739802908765")

# Script generated for node Join
SqlQuery1 = '''
select * from myDataSource join myDataSource2 on
myDataSource.serialNumber = myDataSource2.serialNumber

'''
Join_node1739803550636 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":StepTrainerLanding_node1739802908765, "myDataSource2":curatedcustomers_node1739802909540}, transformation_ctx = "Join_node1739803550636")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select DISTINCT sensorReadingTime,serialNumber,
distanceFromObject from myDataSource

'''
DropFieldsandDuplicates_node1739803929977 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1739803550636}, transformation_ctx = "DropFieldsandDuplicates_node1739803929977")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1739803929977, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739802867448", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739804230872 = glueContext.getSink(path="s3://spark-and-stedi-human-balance-w1/Step_trainer/Trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1739804230872")
AmazonS3_node1739804230872.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="step_trainer_trusted ")
AmazonS3_node1739804230872.setFormat("json")
AmazonS3_node1739804230872.writeFrame(DropFieldsandDuplicates_node1739803929977)
job.commit()
