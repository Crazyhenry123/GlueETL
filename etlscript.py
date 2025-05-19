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

# Read the CSV file
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "etlcsvdbdev", table_name = "devcsv", transformation_ctx = "datasource0")

# If you didn't use a crawler, you can read directly from S3:
# datasource0 = glueContext.create_dynamic_frame.from_options(
#     format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
#     connection_type="s3",
#     format="csv",
#     connection_options={"paths": ["s3://your-bucket/path/to/your/file.csv"]},
#     transformation_ctx="datasource0",
# )

# Perform transformations (example: select specific fields)
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "string", "order_id", "string"), ("date", "string", "date", "date"), ("customer_id", "string", "customer_id", "string"), ("product_id", "string", "product_id", "string"), ("product_name", "string", "product_name", "string"), ("quantity", "long", "quantity", "int"), ("unit_price", "double", "unit_price", "double"), ("total_price", "double", "total_price", "double")], transformation_ctx = "applymapping1")

# Write the result to S3 (example: Parquet format)
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://etl-dev/outputs/"}, format = "parquet", transformation_ctx = "datasink2")

job.commit()
