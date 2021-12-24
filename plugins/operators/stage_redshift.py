from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#F18F01'
    
    sql_placeholder = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 table_name="",
                 aws_credentials_id="",
                 region="us-west-2",
                 redshift_conn_id="redshift",
                 s3_bucket_name="",
                 s3_bucket_key="",
                 extra_params="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table_name
        self.aws_credentials_id = aws_credentials_id
        self.conn_id = redshift_conn_id
        self.bucket_name = s3_bucket_name
        self.bucket_key = s3_bucket_key
        self.region = region
        self.extra_params = extra_params
        
        self.log.info('StageToRedshiftOperator loaded')

    def execute(self, context):
        self.log.info('StageToRedshiftOperator execution')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_conn = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Clean before running
        self.log.info('StageToRedshiftOperator cleaning rows for table {}'.format(self.table))
        redshift_conn.run("DELETE FROM {}".format(self.table))
        
        rendered_key = self.bucket_key.format(**context)
        
        # Create S3 path to retrieve bucket data
        s3_bucket_path = "s3://{}/{}".format(self.bucket_name, rendered_key)

        sql_query = StageToRedshiftOperator.sql_placeholder.format(
            self.table,
            s3_bucket_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.extra_params
        )
        
        self.log.info("StageToRedshiftOperator sql query {}".format(sql_query))
        redshift_conn.run(sql_query)


