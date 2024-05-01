from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data into a dimension table in Redshift.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        conn_id: str = '',
        query: str = '',
        table_name: str = '',
        mode: str = "truncate",
        *args, **kwargs
    ):
        """
        Initialize the LoadDimensionOperator.

        :param conn_id: Connection ID of the Redshift connection.
        :param query: SQL query to extract data.
        :param table_name: Name of the destination dimension table.
        :param mode: The mode of operation (truncate or append).
        """
        super().__init__(*args, **kwargs)
        # Map parameters
        self.conn_id = conn_id
        self.query = query
        self.table_name = table_name
        self.mode = mode

    def execute(self, context):
        """
        Execute the operator.

        :param context: Context dictionary provided by Airflow.
        """
        # Validate parameters
        if self.conn_id.strip() == '':
            raise ValueError(f"No connection provided")
        elif not self.query:
            self.log.info('No query to execute!')
        elif not self.table_name:
            self.log.info('No table name was provided!')
        elif self.mode.lower() not in ['truncate', 'append']:
            raise ValueError("Invalid mode passed. It can either be append or truncate.")
        else:
            try:
                # Establish connection
                postgres = PostgresHook(self.conn_id)
                
                # Execute based on mode
                if self.mode.lower() == "truncate":
                    self.log.info(f'Truncating table {self.table_name}')
                    postgres.run(f'TRUNCATE {self.table_name}')

                # Execute query
                self.log.info(f'Loading DIMENSION table: {self.table_name}')
                postgres.run(f'INSERT INTO {self.table_name} {self.query}')
            except Exception as e:
                self.log.info(f"Exception occurred: {e}")
