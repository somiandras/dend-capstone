from configparser import ConfigParser
import logging

import boto3
from airflow.settings import Session
from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator, Connection
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class CreateRedshiftClusterOperator(BaseOperator):
    ui_color = "#a464d1"

    @apply_defaults
    def __init__(
        self,
        *args,
        aws_credentials_id="aws_default",
        configpath="config/redshift.cfg",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.configpath = configpath
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Create Redshift cluster from config, add details to Airflow
        connections and save `conn_id` as Variable.

        Args:
            configpath (str): path to cluster config file
        """
        config = ConfigParser()
        config.read(self.configpath)
        session = Session()

        cluster_id = config.get("CLUSTER", "CLUSTER_ID")

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
                ClusterType=config.get("CLUSTER", "CLUSTER_TYPE"),
                NodeType=config.get("CLUSTER", "NODE_TYPE"),
                NumberOfNodes=int(config.get("CLUSTER", "NUMBER_OF_NODES")),
                DBName=config.get("CLUSTER", "DB_NAME"),
                Port=int(config.get("CLUSTER", "DB_PORT")),
                MasterUsername=config.get("CLUSTER", "DB_USER"),
                MasterUserPassword=config.get("CLUSTER", "DB_PASSWORD"),
                IamRoles=[config.get("IAM_ROLE", "ARN")],
            )
            logging.info(f"{cluster_id} created")
            logging.info(response)
        except redshift.exceptions.ClusterAlreadyExistsFault:
            logging.info(f"{cluster_id} already exists")
            self.save_connection(session, cluster_id, config)
            session.commit()
        except Exception as e:
            session.rollback()
            raise AirflowFailException(e)
        else:
            self.save_connection(session, cluster_id, config)
            session.commit()

    def save_connection(self, session, cluster_id, config):
        """
        Save connection details to Airflow connections, if there is no
        saved connection with the given `cluster_id` as `conn_id`.

        Args:
            session (SQLAlchemy.Session): Current session object
            cluster_id (str): Cluster identifier of the cluster
            config (ConfigParser): Cluster configuration object
        """
        redshift_connection = (
            session.query(Connection).filter(Connection.conn_id == cluster_id).first()
        )
        if redshift_connection is None:
            logging.info(f"Adding {cluster_id} to Airflow connections")
            session.add(
                Connection(
                    conn_id=cluster_id,
                    conn_type="postgres",
                    login=config.get("CLUSTER", "DB_USER"),
                    password=config.get("CLUSTER", "DB_PASSWORD"),
                    schema=config.get("CLUSTER", "DB_NAME"),
                    port=int(config.get("CLUSTER", "DB_PORT")),
                )
            )
