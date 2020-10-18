import logging

import boto3
from airflow.settings import Session
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class SaveRedshiftHostOperator(BaseOperator):
    ui_color = "#86bdf0"

    @apply_defaults
    def __init__(
        self,
        *args,
        aws_credentials_id="aws_default",
        cluster_identifier="",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.cluster_id = cluster_identifier

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = boto3.client(
            "redshift",
            region_name="us-west-2",
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_access_key=aws_credentials.secret_key,
        )
        logging.info(f"Requesting Redshift cluster endpoint for {self.cluster_id}")
        cluster = redshift.describe_clusters(ClusterIdentifier=self.cluster_id)[
            "Clusters"
        ][0]
        endpoint = cluster["Endpoint"]["Address"]
        session = Session()
        redshift_connection = (
            session.query(Connection)
            .filter(Connection.conn_id == self.cluster_id)
            .first()
        )
        redshift_connection.host = endpoint
        session.commit()
        logging.info(f"Endpoint saved: {endpoint}")
