from __future__ import print_function

import os
import logging
from datetime import timedelta
from pprint import pprint
from tempfile import TemporaryDirectory

from airflow import DAG, configuration
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators import python_operator
from cosmosetl.cli import export_blocks, export_transactions_and_events, get_block_range_for_date


def build_export_dag(
    dag_id,
    provider_uris,
    output_bucket,
    cloud_provider,
    export_start_date,
    notification_emails=None,
    export_schedule_interval='0 0 * * *',
    export_max_workers=10,
    export_batch_size=10,
    export_max_active_runs=None,
    export_retries=5,
    **kwargs
):
    default_dag_args = {
        'dependss_on_past': False,
        'start_date': export_start_date,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': export_retries,
        'retry_delay': timedelta(minutes=5)
    }
    if notification_emails and len(notification_emails) > 0:
        default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

    if export_max_active_runs is None:
        export_max_active_runs = configuration.conf.getint('core', 'max_active_runs_per_dag')
    
    export_blocks_toggle = kwargs.get('export_blocks_toggle')
    export_transactions_and_events_toggle = kwargs.get('export_transactions_and_events_toggle')

    dag = DAG(
        dag_id,
        schedule_interval=export_schedule_interval,
        default_args=default_dag_args,
        max_active_runs=export_max_active_runs
    )

    cloud_storage_hook = GoogleCloudStorageHook(gcp_conn_id='google_cloud_default')

    def export_path(directory, date):
        return 'export/{directory}/block_date={block_date}/'.format(
            directory=directory, block_date=date.strftime('%Y-%m-%d')
        )

    def copy_to_export_path(file_path, export_path):
        logging.info('Calling copy_to_export_path({}, {})'.format(file_path, export_path))
        filename = os.path.basename(file_path)
        upload_to_gcs(
            gcs_hook=cloud_storage_hook,
            bucket=output_bucket,
            object=export_path + filename,
            filename=file_path
        )

    def copy_from_export_path(export_path, file_path):
        logging.info('Calling copy_from_export_path({}, {})'.format(export_path, file_path))
        filename = os.path.basename(file_path)
        download_from_gcs(bucket=output_bucket, object=export_path+filename, filename=file_path)

    def get_block_range(tempdir, date, provider_uri):
        logging.info('Calling get_block_range_for_date({}, {}, ...)'.format(provider_uri, date))
        get_block_range_for_date.callback(
            provider_uri=provider_uri, date=date, output=os.path.join(tempdir, "blocks_meta.txt")
        )
        with open(os.path.join(tempdir, 'blocks_meta.txt')) as block_range_file:
            block_range = block_range_file.read()
            start_block, end_block = block_range.split(',')
        return int(start_block), int(end_block)

    def export_blocks_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)
            
            logging.info('Calling export_blocks({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers
            ))
            
            export_blocks.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                output=os.path.join(tempdir, 'blocks.json')
            )

            copy_to_export_path(
                os.path.join(tempdir, 'blocks_meta.txt'), export_path('blocks_meta', logical_date)
            )
            copy_to_export_path(
                os.path.join(tempdir, 'blocks.json'), export_path('blocks', logical_date)
            )
    
    def export_transactions_and_events_command(logical_date, provider_uri, **kwargs):
        with TemporaryDirectory() as tempdir:
            # blocks_meta reference should be removed or changed
            start_block, end_block = get_block_range(tempdir, logical_date, provider_uri)
            
            logging.info('Calling export_transactions_and_events({}, {}, {}, {}, {}, ...)'.format(
                start_block, end_block, export_batch_size, provider_uri, export_max_workers
            ))
            
            export_transactions_and_events.callback(
                start_block=start_block,
                end_block=end_block,
                batch_size=export_batch_size,
                provider_uri=provider_uri,
                max_workers=export_max_workers,
                transactions_output=os.path.join(tempdir, 'transactions.json'),
                events_output=os.path.join(tempdir, 'events.json')
            )

            copy_to_export_path(
                os.path.join(tempdir, 'blocks_meta.txt'), export_path('blocks_meta', logical_date)
            )
            copy_to_export_path(
                os.path.join(tempdir, 'transactions.json'), export_path('transactions', logical_date)
            )
            copy_to_export_path(
                os.path.join(tempdir, 'events.json'), export_path('events', logical_date)
            )

    def add_export_task(toggle, task_id, python_callable, dependencies=None):
        if toggle:
            operator = python_operator.PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                provide_context=True,
                execution_timeout=timedelta(hours=15),
                dag=dag
            )
            if dependencies is not None and len(dependencies) > 0:
                for dependency in dependencies:
                    if dependency is not None:
                        dependency >> operator
            return operator
        else:
            return None 
    
    export_blocks_task = add_export_task(
        export_blocks_toggle,
        'export_blocks',
        add_provider_uri_fallback_loop(export_blocks_command, provider_uris)
    )

    export_transactions_and_events_task = add_export_task(
        export_transactions_and_events_toggle,
        'export_transactions_and_events',
        add_provider_uri_fallback_loop(export_transactions_and_events_command, provider_uris)
    )

    return dag            


MEGABYTE = 1024 * 1024

def add_provider_uri_fallback_loop(python_callable, provider_uris):
    """Tries each provider uri in provider_uris until the command succeeds"""

    def python_callable_with_fallback(**kwargs):
        for index, provider_uri in enumerate(provider_uris):
            kwargs['provider_uri'] = provider_uri
            try:
                python_callable(**kwargs)
                break
            except Exception as e:
                if index < (len(provider_uris) - 1):
                    logging.exception('An exception occurred. Trying another uri')
                else:
                    raise e

    return python_callable_with_fallback


# Helps avoid OverflowError: https://stackoverflow.com/questions/47610283/cant-upload-2gb-to-google-cloud-storage
# https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
def upload_to_gcs(gcs_hook, bucket, object, filename, mime_type='application/octet-stream'):
    service = gcs_hook.get_conn()
    bucket = service.get_bucket(bucket)
    blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    blob.upload_from_filename(filename)


# Can download big files unlike gcs_hook.download which saves files in memory first
def download_from_gcs(bucket, object, filename):
    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob_meta = bucket.get_blob(object)

    if blob_meta.size > 10 * MEGABYTE:
        blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    else:
        blob = bucket.blob(object)

    blob.download_to_filename(filename)
