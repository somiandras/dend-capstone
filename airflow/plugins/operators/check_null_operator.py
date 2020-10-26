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
        self, *args, redshift_conn_id="redshift", checks=None, **kwargs,
    ):
        """
        Checks null values in given (table, columns) combinations, and
        raised if any of the columns contain null values.

        :param redshift_conn_id: Airflow connection id, defaults to "redshift"
        :type redshift_conn_id: str, optional
        :param checks: table and columns where nulls should be chacked, defaults to None
        :type checks: list of (str, list of strings) tuples, optional
        """
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
