from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import logging
import pandas as pd
from io import StringIO

S3_BUCKET = "music-streaming-data-b"
RAW_DATA_PREFIX = "raw-data/" # main folder
STREAMS_PREFIX = "raw-data/streams/"  # Subfolder
ARCHIVE_PREFIX = "archive/raw-data/"  # Archive folder
GLUE_JOB_NAME = "MusicKPIJob"

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_to_glue_dag',
    default_args=default_args,
    description='Pipeline for a music streaming service',
    schedule_interval=None,
    catchup=False,
)

# Task 1: S3 Sensor - Wait for new files in raw-data folder (including streams)
s3_sensor = S3KeySensor(
    task_id='s3_sensor_task',
    bucket_name=S3_BUCKET,
    bucket_key=[
        f"{RAW_DATA_PREFIX}*.csv",
        f"{STREAMS_PREFIX}*.csv"
    ],
    wildcard_match=True,
    aws_conn_id='aws_default',
    mode='poke',
    poke_interval=30,  # Check every 30 seconds
    timeout=60 * 10,  # Wait up to 10 minutes
    dag=dag,
    deferrable=True # Allows Airflow to sleep until an actual file is found
)

# Define required columns for each file
REQUIRED_COLUMNS = {
    "users.csv": {"user_id", "user_name"},
    "songs.csv": {"track_id", "track_name", "track_genre"},
    "streams.csv": {"track_id", "user_id", "listen_time"},
}

# Task 2: Validation - Ensure required files exist
def validate_files(**kwargs):
    logging.info("Starting validate_files task...")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Log the bucket name and prefixes
    logging.info(f"Checking S3 bucket: {S3_BUCKET}")
    logging.info(f"Checking prefix: {RAW_DATA_PREFIX}")

    # Get all files from both raw-data/ and raw-data/streams/
    files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=RAW_DATA_PREFIX)
    stream_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=STREAMS_PREFIX)
    
    logging.info(f"Files found in {RAW_DATA_PREFIX}: {files}")
    logging.info(f"Files found in {STREAMS_PREFIX}: {stream_files}")

    # Filter out "folders" (keys ending with '/')
    actual_files = [f for f in files + stream_files if not f.endswith('/') and '.' in f]
    stream_files_filtered = [f for f in stream_files if not f.endswith('/') and '.' in f]  # Ensure only files

    logging.info(f"All retrieved files: {actual_files}")
    logging.info(f"Filtered stream files (only files): {stream_files_filtered}")

    if not actual_files:
        logging.warning("No actual files found in the raw-data S3 folder! DAG should not continue.")
        raise ValueError("No valid files found in S3!")

    # Push stream files to XCom for next task
    kwargs['ti'].xcom_push(key='stream_files', value=stream_files_filtered )
    logging.info(f"Stream files pushed to XCom: {stream_files_filtered }")

validate_files_task = PythonOperator(
    task_id='validate_files_task',
    python_callable=validate_files,
    provide_context=True,
    dag=dag,
)

# Task 3: Validating the columns
def validate_columns(**kwargs):

    logging.info("Starting validate_columns task...")

    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Retrieve stream files list from XCom
    ti = kwargs['ti']
    stream_files = ti.xcom_pull(task_ids='validate_files_task', key='stream_files') or []
    logging.info(f"Stream files retrieved from XCom: {stream_files}")

    if not stream_files:
        logging.warning("No stream files found in XCom.")
        raise ValueError("No stream files found. Validation cannot proceed.")

    # Function to check required columns
    def check_columns(file_key, required_columns):
        """Reads the file from S3 and validates column structure"""

        logging.info(f"Checking file: {file_key}")

        obj = s3_hook.get_key(file_key, bucket_name=S3_BUCKET)

        if not obj:
            logging.error(f"File not found in S3: {file_key}")
            raise ValueError(f"File {file_key} not found in S3!")

        content = obj.get()['Body'].read().decode('utf-8')  # Read file content
        df = pd.read_csv(StringIO(content))  # Load into Pandas DataFrame

        logging.info(f"Loading into Pandas DataFrame")

        actual_columns = set(df.columns)
        logging.info(f"Columns in {file_key}: {actual_columns}")

        missing_columns = required_columns - actual_columns

        if missing_columns:
            logging.error(f"File {file_key} is missing columns: {missing_columns}")
            raise ValueError(f"File {file_key} is missing required columns: {missing_columns}")

    # Check columns for users.csv & songs.csv
    logging.info("Validating required columns for users.csv and songs.csv...")
    check_columns(f"{RAW_DATA_PREFIX}users.csv", REQUIRED_COLUMNS["users.csv"])
    check_columns(f"{RAW_DATA_PREFIX}songs.csv", REQUIRED_COLUMNS["songs.csv"])

    # Check ALL stream files (to ensure structure consistency)
    for stream_file in stream_files:
        check_columns(stream_file, REQUIRED_COLUMNS["streams.csv"])

    logging.info("All required files and columns are present. DAG can proceed.")

validate_columns_task = PythonOperator(
    task_id='validate_columns_task',
    python_callable=validate_columns,
    provide_context=True,
    dag=dag,
)

# Task 4: Trigger AWS Glue job
glue_task = GlueJobOperator(
    task_id='glue_job_task',
    job_name=GLUE_JOB_NAME,
    aws_conn_id='aws_default',
    wait_for_completion=True,
    script_location=f"s3://music-streaming-data-b/glue-scripts/MusicKPIJob.py",
    dag=dag,
)

# Task 5: Archive all files in raw-data/ and raw-data/streams/ into date-based folders
def archive_files(**kwargs):

    logging.info("Starting archive_files task...")

    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Get all files from raw-data/ (including raw-data/streams/)
    logging.info(f"Checking S3 bucket: {S3_BUCKET}")
    logging.info(f"Listing files in: {RAW_DATA_PREFIX}")

    all_files = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=RAW_DATA_PREFIX) or []
    files = [f for f in all_files if not f.endswith('/')]  # Only process files

    if not files:
        logging.info("No files found to archive. Exiting task.")
        return

    # Get today's date in YYYY-MM-DD format
    today_date = datetime.today().strftime('%Y-%m-%d')
    logging.info(f"Using archive folder: {ARCHIVE_PREFIX}{today_date}/")

    for file in files:
        # Preserve folder structure and move to a date-based archive folder
        archive_key = file.replace(RAW_DATA_PREFIX, f"{ARCHIVE_PREFIX}{today_date}/")
        logging.info(f"Archiving file: {file} → {archive_key}")
        
        # Check if file exists before copying
        if not s3_hook.check_for_key(file, bucket_name=S3_BUCKET):
            logging.error(f"File not found: {file}")
            continue

        try:
            # Move file to archive
            s3_hook.copy_object(
            source_bucket_name=S3_BUCKET,
            source_bucket_key=file,
            dest_bucket_name=S3_BUCKET,
            dest_bucket_key=archive_key
            )
            logging.info(f"Successfully copied {file} to {archive_key}")

            # Delete original file after copying
            s3_hook.delete_objects(S3_BUCKET, keys=[file])
            logging.info(f"Deleted original file: {file}")
        
        except Exception as e:
            logging.error(f"Error archiving {file}: {str(e)}")

        logging.info(f"Archived file {file} to {archive_key}")

    # Ensure 'raw-data/streams/' folder remains
    logging.info(f"Checking if {STREAMS_PREFIX} is empty...")
    existing_keys = s3_hook.list_keys(bucket_name=S3_BUCKET, prefix=STREAMS_PREFIX) or []

    if not existing_keys:  
        # If streams folder is empty, create a dummy marker (zero-byte file)
        placeholder_key = f"{STREAMS_PREFIX}.keep"
        s3_hook.load_string("", key=placeholder_key, bucket_name=S3_BUCKET)
        logging.info(f"Preserved empty folder with placeholder: {placeholder_key}")
        logging.info(f"Preserved empty folder: {STREAMS_PREFIX}")

    logging.info("Archive_files task completed successfully.")

archive_task = PythonOperator(
    task_id='archive_files_task',
    python_callable=archive_files,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
# s3_sensor >> validate_files_task >> validate_columns_task >> glue_task >> archive_task
s3_sensor >> validate_files_task >> validate_columns_task >> archive_task