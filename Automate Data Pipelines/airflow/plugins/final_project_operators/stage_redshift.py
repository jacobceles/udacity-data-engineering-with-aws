from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator to stage data from S3 to Redshift.
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, 
                redshift_conn_id: str = '',
                aws_conn_id: str = '',
                table_name: str = '',
                s3_bucket: str = '',
                s3_key: str = '',
                region: str = '',
                data_format: str = '',
                truncate_table: bool = False,
                *args, **kwargs):
        """
        Initialize the StageToRedshiftOperator.

        :param redshift_conn_id: Redshift connection ID.
        :param aws_conn_id: AWS connection ID.
        :param table_name: Redshift table name.
        :param s3_bucket: S3 bucket name.
        :param s3_key: S3 key (can contain placeholders for context).
        :param region: AWS region.
        :param data_format: Data format (e.g., JSON).
        :param truncate_table: Whether to truncate the table before copying data.
        """
        super().__init__(*args, **kwargs)
        # Map parameters
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_format = data_format
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        Execute the operator.

        :param context: Airflow context.
        """
        # Check if data format is JSON
        if "JSON" not in self.data_format.upper():
            raise ValueError("Data Format is not JSON!")
        
        # Initialize hooks
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        creds = aws_hook.get_credentials()
        
        # Generate S3 path
        s3_path = f's3://{self.s3_bucket}/{self.s3_key.format(**context)}'
        
        # Generate final copy command
        final_command = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
            REGION '{}'
        """.format(
            self.table_name, s3_path, 
            creds.access_key, creds.secret_key, 
            self.data_format, self.region
        )

        # Truncate table if specified
        if self.truncate_table:
            self.log.info(f'Truncating table {self.table_name}')
            redshift_hook.run(f'TRUNCATE {self.table_name}')
        
        # Log and execute copy command
        self.log.info(f'Copying data from {s3_path} to the Redshift table {self.table_name}')
        redshift_hook.run(final_command)
