Load_data_from_S3_to_Redshift without using Boto3
This program use airflow hooks to upload files to aws S3 , then airflow operators to load data to redshift.

requirement :  - configuration airflow UI : { 'conn_id':'aws_default', 
					      'connection type': 'Amazon Web Service',
					      'Extra':'{'aws_access_key_id':YOUR_KEY_ID, 'aws_secret_access_key':YOUR_SECRET_KEY}'
					     }# Load_data_from_S3_to_Redshift
