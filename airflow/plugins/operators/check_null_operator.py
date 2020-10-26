import logging
import re

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException


class CheckNullValuesOperator(BaseOperator):

    ui_color = "#75e1ff"

    @apply_defaults
    def __init__(
        self, *args, redshift_conn_id="redshift", query=None, checks=None, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        postgres_hook = PostgresHook(self.redshift_conn_id)
        query_template = """
            select count(*)
            from {table}
            where {filters};
        """

        for (table, cols) in self.checks:
            filters = " or ".join([f"{col} is null" for col in cols])
            table_name = f'stage."{table}_{context["ds"]}"'
            query = query_template.format(table=table_name, filters=filters)
            logging.info(query)
            results = postgres_hook.get_records(query)
            if results is None:
                raise AirflowFailException(
                    f"Quality check did not return any results: {query.strip}"
                )
            elif results[0][0] != 0:
                raise AirflowFailException(
                    f"{results[0][0]} rows with disallowed NULLs in {table}"
                )
            else:
                logging.info(f"NULL check passed for {table}")
