import os
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer

default_arguments = {'owner': 'airflow', 'start_date': days_ago(1)}


def upload_files_to_s3_bucket(filename,bucket_name,region_name):
    hook = S3Hook(aws_conn_id = 'aws_default')
    hook.create_bucket(bucket_name, region_name)
    try:
        i = 1
        for path, subdirs, files in os.walk(filename):
            for file in files:
                hook.load_file(os.path.join(path, file), file, bucket_name)
                print('file {} uploaded successfully '.format(i))
                i += 1
    except Exception as err:
        print(err)


def list_bucket(bucket_name, prefix, delimiter,**kwargs):
    hook = S3Hook(aws_conn_id='s3_default')
    list_objects = hook.list_keys(bucket_name, prefix, delimiter)
    return list_objects


def move_files_from_s3_to_redshift(db, table_, s3_bucket_, copy_options_,task_, **kwargs):

    storage_objects = kwargs["ti"].xcom_pull(task_ids='list_files')
    for storage_object in storage_objects:
        s3_key_ = storage_object
        S3ToRedshiftTransfer(schema=db,
                             table=table_,
                             s3_bucket=s3_bucket_,
                             s3_key=s3_key_,
                             copy_options=copy_options_,
                             task_id=task_)


with DAG('aaaa-test_airflow',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_arguments
         ) as dag:

    upload_to_s3 = PythonOperator(task_id='upload_to_s3',
                                  python_callable=upload_files_to_s3_bucket,
                                  op_kwargs= {'filename':'/c/Users/kon-boot/Desktop/project/s3',
                                              'bucket_name':'airflowbuckettest',
                                              'region_name':'us-east-1'},)
    list_files = PythonOperator(task_id='list_files',
                                python_callable=list_bucket,
                                op_kwargs={'bucket_name':'airflowbuckettest',
                                           'prefix':'',
                                           'delimiter':''},
                                )

    to_rdsh = PythonOperator(task_id='to_rdsh',
                                  python_callable=move_files_from_s3_to_redshift,
                                  op_kwargs={'db':'PUBLIC',
                                             'table_':'lc_logisticcompany',
                                             's3_bucket_':'airflowbuckettest',
                                             'copy_options_':["csv"],
                                             'task_':'to_rdsh'},
                             provide_context=True,
                                  )
upload_to_s3 >> list_files >> to_rdsh
