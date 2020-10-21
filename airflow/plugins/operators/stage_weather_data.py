import logging
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults
import pendulum


class StageWeatherData(BaseOperator):
    ui_color = "#75e1ff"

    def __init__(
        self,
        *args,
        aws_credentials_id="aws_default",
        redshift_conn_id="redshift_default",
        table="",
        s3_bucket=None,
        s3_key=None,
        manifest_bucket="dend-capstone-somi",
        min_date=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.manifest_bucket = manifest_bucket
        self.table = table
        self.min_date = min_date

    def execute(self, context):
        end_date = context["execution_date"]
        try:
            start_date = context["prev_execution_date_success"].add(days=1)
        except AttributeError:
            start_date = pendulum.instance(self.min_date)

        logging.info(f"Importing from {start_date} to {end_date}")

        manifest_path = self.create_manifest_file(start_date, end_date)

        aws_hook = AwsHook(self.aws_credentials_id)
        creds = aws_hook.get_credentials()
        source_path = f"s3://{self.s3_bucket}/{self.s3_key}/"
        logging.info(f"Copying data from {source_path} to {self.table}")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(
            f"""
                -- Clear staging table
                truncate {self.table};

                -- Copy from manifest
                copy {self.table} (station, name, date, awnd, prcp, snow, tmax, tmin)
                from '{manifest_path}'
                access_key_id '{creds.access_key}'
                secret_access_key '{creds.secret_key}'
                region 'us-east-1'
                json 'auto ignorecase'
                timeformat 'epochmillisecs'
                manifest;
            """
        )
        logging.info(f"Copied data from {source_path}")

    def create_manifest_file(self, start_date, end_date):
        """
        Filter csv files in S3 by dates and upload manifest to project bucket.

        :param start_date: First date to be included in import.
        :type start_date: pendulum.Pendulum or datetime.datetime
        :param end_date: Last date to be included in import.
        :type end_date: pendulum.Pendulum or datetime.datetime
        """
        logging.info("Creating manifest file")
        period = pendulum.instance(end_date) - pendulum.instance(start_date)
        manifest = dict(
            entries=[
                {
                    "url": f"s3://{self.s3_bucket}/weather/{date.to_date_string()[:7]}/weather-{date.to_date_string()}.json",
                    "mandatory": False,
                }
                for date in period.range("days")
            ]
        )

        s3_hook = S3Hook()
        logging.info("Saving new manifest")
        s3_hook.load_bytes(
            json.dumps(manifest, indent=2).encode(),
            key="weather_manifest.json",
            bucket_name=self.manifest_bucket,
            replace=True,
        )
        manifest_path = f"s3://{self.manifest_bucket}/weather_manifest.json"
        logging.info(f"Saved manifest to {manifest_path}")
        return manifest_path
