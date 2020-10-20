import logging
import json

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults
import pendulum


class StageTripData(BaseOperator):
    """
    Operator for staging NYC taxi trip data to Redshift. Loads data for
    period since last successful run if there is one. If not, it imports
    starting from `min_date`.
    """

    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        *args,
        aws_credentials_id="aws_default",
        redshift_conn_id="redshift_default",
        table="",
        s3_bucket="nyc-tlc",
        s3_key="trip data",
        project_bucket="dend-capstone-somi",
        min_date=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.project_bucket = project_bucket
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
                copy {self.table}
                from '{manifest_path}'
                access_key_id '{creds.access_key}'
                secret_access_key '{creds.secret_key}'
                region 'us-east-1'
                csv
                delimiter ','
                ignoreheader 1
                manifest;

                -- Remove trips that fall outside of date range
                delete from {self.table}
                where
                    tpep_pickup_datetime < '{start_date.to_date_string()}'::date or
                    tpep_pickup_datetime > '{end_date.to_date_string()}'::date;
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
                    "url": f"s3://nyc-tlc/trip data/yellow_tripdata_{month.to_date_string()[:7]}.csv",
                    "mandatory": False,
                }
                for month in period.range("months")
            ]
        )

        s3_hook = S3Hook()
        logging.info("Saving new manifest")
        s3_hook.load_bytes(
            json.dumps(manifest, indent=2).encode(),
            key="trip_manifest.json",
            bucket_name=self.project_bucket,
            replace=True,
        )
        manifest_path = f"s3://{self.project_bucket}/trip_manifest.json"
        logging.info(f"Saved manifest to {manifest_path}")
        return manifest_path
