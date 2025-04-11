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

# Script generated for node Step Trainer Trusted 
StepTrainerTrusted_node1739808390842 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="step_trainer_trusted ", transformation_ctx="StepTrainerTrusted_node1739808390842")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1739808391894 = glueContext.create_dynamic_frame.from_catalog(database="stedi-human-balance", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1739808391894")

# Script generated for node Join and Drop Duplicates
SqlQuery0 = '''
select DISTINCT * from myDataSource join myDataSource2 on
myDataSource2.timestamp = myDataSource.sensorreadingtime
'''
JoinandDropDuplicates_node1739808687681 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":StepTrainerTrusted_node1739808390842, "myDataSource2":AccelerometerTrusted_node1739808391894}, transformation_ctx = "JoinandDropDuplicates_node1739808687681")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinandDropDuplicates_node1739808687681, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739808370343", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1739809249835 = glueContext.getSink(path="s3://spark-and-stedi-human-balance-w1/Machine Learning/Curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1739809249835")
MachineLearningCurated_node1739809249835.setCatalogInfo(catalogDatabase="stedi-human-balance",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1739809249835.setFormat("json")
MachineLearningCurated_node1739809249835.writeFrame(JoinandDropDuplicates_node1739808687681)
job.commit()
