import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


source_path = "s3://gluems/input/info.csv"
destination_path = "s3://gluems/output/data.csv"

input = spark.read.format("csv").option("header", "true").load(source_path)
output = input.write.format("csv").mode("overwrite").save(destination_path)
spark.stop()
job.commit()