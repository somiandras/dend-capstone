import logging

import boto3
from airflow.settings import Session
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class SaveRedshiftHostOperator(BaseOperator):
    """
    Request endpoint URL for cluster_id and save Redshift connection.
    """

    ui_color = "#75e1ff"

    @apply_defaults
    def __init__(
        self,
        config,
        *args,
        aws_credentials_id="aws_default",
        cluster_identifier="",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.cluster_id = cluster_identifier
        self.config = config

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = boto3.client(
            "redshift",
            region_name="us-west-2",
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_access_key=aws_credentials.secret_key,
        )
        cluster_id = self.config.get("CLUSTER", "CLUSTER_ID")
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
        if redshift_connection is None:
            logging.info(f"Adding {cluster_id} to Airflow connections")
            session.add(
                Connection(
                    conn_id="redshift_default",
                    conn_type="postgres",
                    login=self.config.get("CLUSTER", "DB_USER"),
                    password=self.config.get("CLUSTER", "DB_PASSWORD"),
                    schema=self.config.get("CLUSTER", "DB_NAME"),
                    port=int(self.config.get("CLUSTER", "DB_PORT")),
                    host=endpoint,
                )
            )
        else:
            redshift_connection.host = endpoint
        session.commit()
        logging.info(f"Connection saved: {endpoint}")
