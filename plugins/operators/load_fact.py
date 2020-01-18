from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_query = """ INSERT INTO {}{};COMMIT; """
    
    @apply_defaults
    def __init__(self,redshift_id="",table="",load_sql="",*args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_id = redshift_id
        self.table = table
        self.load_sql = load_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_id)        
        if self.mode == "append":
            self.log.info("Loading fact table in Redshift")
            sql_format = LoadFactOperator.insert_query.format(self.table, self.load_sql)
        elif self.mode == "delete":
             self.log.info("Delete data from Redshift table")
             redshift.run("DELETE FROM {}".format(self.table))
        redshift.run(sql_format)
        
        