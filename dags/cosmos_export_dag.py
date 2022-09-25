from __future__ import print_function
from cosmosetl_airflow.build_export_dag import build_export_dag
from cosmosetl_airflow.variables import NETWORK_NAME, read_export_dag_vars


DAG = build_export_dag(
    dag_id=f'{NETWORK_NAME}_export_dag',
    **read_export_dag_vars(
        var_prefix=f'{NETWORK_NAME}_',
        export_start_date='2022-09-01',
        export_schedule_interval='0 0 * * *',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
    )
)
