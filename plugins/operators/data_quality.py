from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        
    def execute(self, context):
        self.log.info('DataQualityOperator running')
        
        redshift_conn = PostgresHook(postgres_conn_id=self.conn_id)
        
        # Loop through data quality checks
        for c in self.dq_checks:
           
            records = redshift_conn.get_records(c.get('check_sql'))[0]
     
            if check.get('expected_result') != records[0]:
                raise ValueError("Data quality check failed for check {}".format(c))
            
            self.log.info("Data quality for check {} passed".format(c))