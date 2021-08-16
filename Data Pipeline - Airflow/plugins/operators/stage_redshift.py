from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id,
                 redshift_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f"DELETE FROM {self.table}")
        
        self.log.info("Copying data from S3 to Redshift")
        
        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_s3_key}")
        
        cmd = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' \
            SECRET_ACCESS_KEY '{credentials.secret_key}' FORMAT AS JSON '{self.json_path}'"
        
        redshift.run(cmd)





