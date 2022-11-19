from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago

from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator

default_args = {
    "owner": "sanchit",
    "sla": timedelta(hours=1),
}

config = Variable.get("config", deserialize_json=True)
region = config["region"]
s3_bucket = config["s3_bucket"]
glue_db = config["glue_db"]
table = "test_table"


# TODO: Move to plugins
class TemplatedArgsGlueOperator(AwsGlueJobOperator):
    template_fields = ("script_args",)


with DAG(
    "dynamodb-table-export",
    default_args=default_args,
    description="Export DynamoDB table to S3 in parquet format",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    params={
        "athena_output_location": f"s3://{s3_bucket}/athena-output/{table}",
        "table": table
    }
) as dag:
    dynamodb_to_s3_export = TemplatedArgsGlueOperator(
        task_id="dynamodb_to_s3_export",
        job_name="dynamodb-to-s3-export",
        num_of_dpus=2,
        region_name=region,
        script_args={"--snapshot_ts": "{{ execution_date.int_timestamp }}"},
    )

    add_new_partition = AWSAthenaOperator(
        task_id="add_new_partition",
        query="""
          ALTER TABLE {{ params.table }} ADD PARTITION (snapshot_ts = '{{ execution_date.int_timestamp }}')
          LOCATION 's3://{{ var.json.config.s3_bucket }}/{{ params.table }}/snapshot_ts={{ execution_date.int_timestamp }}'
        """,
        database=glue_db,
        output_location="{{ params.athena_output_location}}",
    )

    update_latest_view = AWSAthenaOperator(
        task_id="update_latest_view",
        query="""
          CREATE OR REPLACE VIEW {{ params.table }}_latest AS
          SELECT * from {{ params.table }}
          WHERE snapshot_ts = '{{ execution_date.int_timestamp }}'
        """,
        database=glue_db,
        output_location="{{ params.athena_output_location}}",
    )

dynamodb_to_s3_export >> add_new_partition >> update_latest_view
