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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739791925201 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1739791925201")

# Script generated for node Customer Trusted
CustomerTrusted_node1739791924417 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1739791924417")

# Script generated for node Join Customer 
JoinCustomer_node1739791928833 = Join.apply(frame1=AccelerometerTrusted_node1739791925201, frame2=CustomerTrusted_node1739791924417, keys1=["user"], keys2=["email"], transformation_ctx="JoinCustomer_node1739791928833")

# Script generated for node Drop Fields and duplicates
SqlQuery0 = '''
select DISTINCT customerName,email,phone,birthDay,serialNumber,
registrationDate,lastUpdateDate,shareWithResearchAsOfDate,
shareWithPublicAsOfDate,shareWithFriendsAsOfDate 
from myDataSource
'''
DropFieldsandduplicates_node1739791930166 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomer_node1739791928833}, transformation_ctx = "DropFieldsandduplicates_node1739791930166")

# Script generated for node Customer Curated 
EvaluateDataQuality().process_rows(frame=DropFieldsandduplicates_node1739791930166, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739791918777", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1739791932918 = glueContext.getSink(path="s3://spark-and-stedi-human-balance-w1/Customer/Curated /", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1739791932918")
CustomerCurated_node1739791932918.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="customers_curated")
CustomerCurated_node1739791932918.setFormat("json")
CustomerCurated_node1739791932918.writeFrame(DropFieldsandduplicates_node1739791930166)
job.commit()
