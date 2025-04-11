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

# Script generated for node Customer Landing 
CustomerLanding_node1739790521363 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="customer_landing", transformation_ctx="CustomerLanding_node1739790521363")

# Script generated for node share with research
SqlQuery0 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
sharewithresearch_node1739790555043 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1739790521363}, transformation_ctx = "sharewithresearch_node1739790555043")

# Script generated for node Customer trusted
EvaluateDataQuality().process_rows(frame=sharewithresearch_node1739790555043, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739790517200", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customertrusted_node1739790561853 = glueContext.getSink(path="s3://spark-and-stedi-human-balance-w1/Customer/Trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusted_node1739790561853")
Customertrusted_node1739790561853.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="customer_trusted")
Customertrusted_node1739790561853.setFormat("json")
Customertrusted_node1739790561853.writeFrame(sharewithresearch_node1739790555043)
job.commit()
