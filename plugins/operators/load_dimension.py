from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    sql_placeholder = """
        INSERT INTO {}
        {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 table_name="",
                 redshift_conn_id="redshift",
                 sql_content="",
                 mode="delete",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
       
        self.table=table_name
        self.conn_id = redshift_conn_id
        self.mode = mode
        self.sql_content=sql_content

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift_conn = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.mode == "delete":
            # Clean before running
            self.log.info('LoadDimensionOperator cleaning rows for table {}'.format(self.table))
            redshift_conn.run("DELETE FROM {}".format(self.table))
        
        sql_query = LoadDimensionOperator.sql_placeholder.format(
         self.table,
         self.sql_content
        )
        
        self.log.info("LoadDimensionOperator sql query {}".format(sql_query))
        redshift_conn.run(sql_query)
