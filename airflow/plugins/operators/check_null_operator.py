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
        self,
        *args,
        redshift_conn_id="redshift",
        query=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        dag_folder = context["conf"].get("core", "dags_folder")
        filepath = f"{dag_folder}/{self.query}"
        with open(filepath) as f:
            querystr = f.read().strip()

        postgres_hook = PostgresHook(self.redshift_conn_id)
        splitted = [q for q in querystr.split(";") if len(q) > 1]
        for query in splitted:
            logging.info(query)
            table_name_regex = re.compile(r"select\scount\(\*\)\sfrom\s(.*)\swhere.*")
            table_name = table_name_regex.match(query.strip()).group(1)
            results = postgres_hook.get_records(f"{query};")
            if results is None:
                raise AirflowFailException(
                    f"Quality check did not return any results: {query.strip}"
                )
            elif results[0][0] != 0:
                raise AirflowFailException(
                    f"{results[0][0]} rows with disallowed NULLs in {table_name}"
                )
            else:
                logging.info(f"NULL check passed for {table_name}")
