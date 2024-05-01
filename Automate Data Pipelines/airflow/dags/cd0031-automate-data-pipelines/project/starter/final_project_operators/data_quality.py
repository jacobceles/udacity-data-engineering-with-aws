from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List, Dict, Any

class DataQualityOperator(BaseOperator):
    """
    Operator for running data quality checks on a PostgreSQL database.

    :param conn_id: The connection ID for the PostgreSQL database.
    :param test_case_queries: List of dictionaries containing test cases.
                               Each dictionary should contain 'sql_statement' and 'expected_result' keys.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id: str = '',
                 test_case_queries: List[Dict[str, Any]] = [],
                 *args, **kwargs):
        """
        Initialize DataQualityOperator.

        :param conn_id: The connection ID for the PostgreSQL database.
        :param test_case_queries: List of dictionaries containing test cases.
                                   Each dictionary should contain 'sql_statement' and 'expected_result' keys.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.conn_id = conn_id
        self.test_case_queries = test_case_queries

    def execute(self, context):
        """
        Execute data quality checks.

        :param context: The task execution context.
        """
        if self.conn_id.strip() == '':
            raise ValueError("No connection provided")
        elif not self.test_case_queries:
            self.log.info('No tests to execute!')
        else:
            try:
                for test_case in self.test_case_queries:
                    sql_statement = test_case['sql_statement']
                    expected_result = test_case['expected_result']
                    
                    postgres = PostgresHook(postgres_conn_id=self.conn_id)
                    actual_result = postgres.get_records(sql_statement)

                    if actual_result != expected_result:
                        raise ValueError(f"Test case failed: {sql_statement}. Expected {expected_result}, got {actual_result}")
                self.log.info('All given test cases passed!')
            except Exception as e:
                self.log.info(f"Exception occurred: {e}")
