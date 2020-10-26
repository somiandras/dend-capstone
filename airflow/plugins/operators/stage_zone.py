import logging
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults
import pendulum


class StageZoneData(BaseOperator):
    ui_color = "#75e1ff"

    def __init__(
        self,
        *args,
        aws_credentials_id="aws_default",
        redshift_conn_id="redshift_default",
        table="",
        s3_bucket=None,
        s3_key=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table

    def execute(self, context):
        logging.info(f"Importing zone data")

        aws_hook = AwsHook(self.aws_credentials_id)
        creds = aws_hook.get_credentials()
        source_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        logging.info(f"Copying data from {source_path} to {self.table}")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(
            f"""
                copy {self.table}
                from '{source_path}'
                access_key_id '{creds.access_key}'
                secret_access_key '{creds.secret_key}'
                region 'us-east-1'
                csv
                delimiter ','
                ignoreheader 1;
            """
        )
        logging.info(f"Copied data from {source_path}")
