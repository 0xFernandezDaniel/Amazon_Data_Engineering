from airflow import DAG
from datetime import timedelta, datetime
import json
import requests
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# Load API configs
try:
    with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
        api_host_key = json.load(config_file)
except FileExistsError:
    print('The config file was not found')
except json.JSONDecodeError:
    print('Error decoding the JSON file')

# Set now's time
now = datetime.now()
dt_now_string = now.strftime('%d%m%Y%H%S')  # for output file path

# Define the final S3 bucket
s3_bucket = 'cleaned-data-zone-csv-bucket'

# Extract data from API
def extract_amazon_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']

    all_asins = set()  # Use a set to store unique ASINs
    page = 1

    while True:
        querystring['page'] = page  # Update the page parameter
        response = requests.get(url, headers=headers, params=querystring)
        response_data = response.json()  # Convert request response to JSON

        # Check if there's data in the current page
        if not response_data or len(response_data) == 0:
            break  # Exit loop if no more data
        
        # Add ASINs to the set to ensure uniqueness
        for item in response_data:
            if 'asin' in item:  # Make sure 'asin' exists in the response
                all_asins.add(item['asin'])

        page += 1  # Move to the next page

    # Convert the set back to a list for further processing
    unique_asins = list(all_asins)

    # Specify output file path
    output_file_path = f"/home/ubuntu/airflow/response_{dt_string}.json"
    file_str = f"response_{dt_string}.csv"

    # Write unique ASINs to a file
    with open(output_file_path, 'w') as output_file:
        json.dump(unique_asins, output_file, indent=4)
    
    output_list = [output_file_path, file_str]
    return output_list

# Default args
default_args = {
    'owner': 'Daniel Fernandez',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 9),
    'email': 'danfernandez2201@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

# Create DAG
with DAG('amazon_analytics_dag',
         default_args=default_args,
         schedule_interval='@daily', #'*/1 * * * *'
         catchup=False) as dag:

    extract_amazon_data_var = PythonOperator(
        task_id='tsk_extract_amazon_data_var',
        python_callable=extract_amazon_data,
        op_kwargs={
            'url': 'https://real-time-amazon-data.p.rapidapi.com/search',  
            'querystring': {"query": " ", "country":"US", "seller_id":"ALRPFGEN62HZC", "product_condition": "NEW"},
            'headers': api_host_key,
            'date_string': dt_now_string  # Fixed key name
        }
    )

    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull(task_ids="tsk_extract_amazon_data_var")[0] }} s3://myawsbucket809test',
    )
    
    is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key='{{ ti.xcom_pull(task_ids="tsk_extract_amazon_data_var")[1] }}',
        bucket_name=s3_bucket,
        aws_conn_id='aws_s3_conn',
        wildcard_match=False,
        timeout=60,  # seconds
        poke_interval=5,  # interval for S3 checks (seconds)
    )

    # Load data from S3 to existing Redshift table
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id='transfer_s3_to_redshift',
        aws_conn_id='aws_s3_conn',
        redshift_conn_id='conn_id_redshift',
        s3_bucket=s3_bucket,
        s3_key='{{ ti.xcom_pull("tsk_extract_amazon_data_var")[1] }}',
        schema='PUBLIC',
        table='amazon_data',
        copy_options=['csv IGNOREHEADER 1'],
    )

    extract_amazon_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift

