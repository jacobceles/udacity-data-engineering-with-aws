from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load data into a fact table in Redshift or PostgreSQL.

    :param conn_id: Connection ID of the database to connect to.
    :param query: SQL query to extract data for insertion.
    :param table_name: Name of the fact table to load data into.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id: str = '',
                 query: str = '',
                 table_name: str = '',
                 *args, **kwargs):
        """
        Initialize the operator with necessary parameters.

        :param conn_id: Connection ID of the database.
        :param query: SQL query to execute.
        :param table_name: Name of the target table.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map parameters
        self.conn_id = conn_id
        self.query = query
        self.table_name = table_name

    def execute(self, context):
        """
        Execute the operator.

        :param context: Context dictionary, containing information related to the execution.
        """
        try:
            # Establish connection to the database
            postgres = PostgresHook(self.conn_id)
            
            # Log the start of the loading process
            self.log.info(f'Loading FACT table: {self.table_name}')
            
            # Execute the SQL query to insert data into the fact table
            postgres.run(f'INSERT INTO {self.table_name} {self.query}')
        except Exception as e:
            # Log any exceptions that occur during execution
            self.log.info(f"Exception occurred: {e}")
