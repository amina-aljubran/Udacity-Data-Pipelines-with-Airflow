from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = """ TRUNCATE TABLE {}; INSERT INTO {} {}; COMMIT;"""

    @apply_defaults
    def __init__(self, redshift_id="", table="", load_query="", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_id = redshift_id
        self.table = table
        self.load_query = load_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_id)
        if self.mode == "append":
            self.log.info(f"Loading dimension table {self.table} in Redshift")
            sql_format = LoadDimensionOperator.insert_sql.format(self.table, self.table, self.load_query)
        elif self.mode == "delete":
             self.log.info("Delete data from Redshift table")
             redshift.run("DELETE FROM {}".format(self.table))
        redshift.run(sql_format)
        
    