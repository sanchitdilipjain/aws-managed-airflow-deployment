import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "s3_location",
        "table_name",
        "table_throughput_read_percent",
        "snapshot_ts",
        "workers",
    ],
)
job_name = args["JOB_NAME"]
s3_location = args["s3_location"]
table_name = args["table_name"]
table_throughput_read_percent = args["table_throughput_read_percent"]
snapshot_ts = args["snapshot_ts"]
workers = int(args["workers"])

table_splits = (workers - 1) * 8 if workers > 1 else 1

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(job_name, args)

table = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={
        "dynamodb.input.tableName": table_name,
        "dynamodb.throughput.read.percent": table_throughput_read_percent,
        "dynamodb.splits": str(table_splits),
    },
)

glueContext.write_dynamic_frame.from_options(
    frame=table,
    connection_type="s3",
    connection_options={
        "path": f"{s3_location}/snapshot_ts={snapshot_ts}",
        "partitionKeys": "snapshot_ts",
    },
    format="parquet",
)

job.commit()
