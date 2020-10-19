from configparser import ConfigParser
import logging

import boto3
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class CreateRedshiftClusterOperator(BaseOperator):
    """
    Create Redshift cluster from config.
    """

    ui_color = "#a464d1"

    @apply_defaults
    def __init__(
        self,
        config,
        *args,
        aws_credentials_id="aws_default",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.config = config
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        cluster_id = self.config.get("CLUSTER", "CLUSTER_ID")

        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()

        logging.info(f"Creating redshift cluster {cluster_id}...")
        redshift = boto3.client(
            "redshift",
            region_name="us-west-2",
            aws_access_key_id=aws_credentials.access_key,
            aws_secret_access_key=aws_credentials.secret_key,
        )
        try:
            response = redshift.create_cluster(
                ClusterIdentifier=cluster_id,
                ClusterType=self.config.get("CLUSTER", "CLUSTER_TYPE"),
                NodeType=self.config.get("CLUSTER", "NODE_TYPE"),
                NumberOfNodes=int(self.config.get("CLUSTER", "NUMBER_OF_NODES")),
                DBName=self.config.get("CLUSTER", "DB_NAME"),
                Port=int(self.config.get("CLUSTER", "DB_PORT")),
                MasterUsername=self.config.get("CLUSTER", "DB_USER"),
                MasterUserPassword=self.config.get("CLUSTER", "DB_PASSWORD"),
                IamRoles=[self.config.get("IAM_ROLE", "ARN")],
            )
            logging.info(f"{cluster_id} created")
            logging.info(response)
        except redshift.exceptions.ClusterAlreadyExistsFault:
            logging.info(f"{cluster_id} already exists")
        except Exception as e:
            raise AirflowFailException(e)
