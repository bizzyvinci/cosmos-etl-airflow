# Cosmos ETL Airflow

## Setting up Airflow DAGs using Google Cloud Composer

### Create BigQuery Datasets
To do: for loading dataset

### Create Google Cloud Storage bucket

- Create a new Google Storage bucket to store exported files https://console.cloud.google.com/storage/browser

### Create Google Cloud Composer environment

Create a new Cloud Composer environment:

Create variables in Airflow (**Admin > Variables** in the UI):

| Variable                                      | Description                                       |
|-----------------------------------------------|---------------------------------------------------|
| {NETWORK_NAME}_output_bucket                  | GCS bucket to store exported files                |
| {NETWORK_NAME}_provider_uris                  | Comma separated URIs of archive tendermint nodes  |
| {NETWORK_NAME}_export_blocks_toggle           | true or yes if you want to export blocks          |
| {NETWORK_NAME}_export_transactions_and_events_toggle  | true or yes if you want to export transactions and events |

Check `NETWORK_NAME` and other variables in `dags/cosmosetl_airflow/variables.py`.

### Upload DAGs

```bash
> ./upload_dags.sh <airflow_bucket>
```
