import logging
import re

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowFailException


class CheckUniqueValuesOperator(BaseOperator):

    ui_color = "#75e1ff"

    @apply_defaults
    def __init__(
        self, *args, redshift_conn_id="redshift", checks=None, **kwargs,
    ):
        """
        Checks uniqueness in given (table, columns) combinations, and
        raised if any of the rows are not unique.

        :param redshift_conn_id: Airflow connection id, defaults to "redshift"
        :type redshift_conn_id: str, optional
        :param checks: table and columns where uniiqueness should be checked, defaults to None
        :type checks: list of (str, list of strings) tuples, optional
        """
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        postgres_hook = PostgresHook(self.redshift_conn_id)
        query_template = """
            select {group_cols}, count(*) as count
            from {table}
            group by {group_cols}
            having count > 1;
        """

        for (table, cols) in self.checks:
            group_cols = ", ".join(cols)
            table_name = f'stage."{table}_{context["ds"]}"'
            query = query_template.format(table=table_name, group_cols=group_cols)
            logging.info(query)
            results = postgres_hook.get_records(query)
            if len(results) == 0:
                logging.info(f"Unique column check passed for {table}")
            else:
                raise AirflowFailException(
                    f"Uniqueness failed for {table} for columns {group_cols}: {results}"
                )
