# gcp-export-billing-bq

This repo includes python script `formatter.py` and App Engine script `cron.py` to insert export-billing data in BigQuery.

## Auto insert

You can insert export-billing data in BigQuery automatically using App Engine Cron.

Before start deployment, please make sure following variables.

| Environment Variable| Description                       |
|:--------------------|:----------------------------------|
| project_name        | Your Google Platform project name |
| bucket_name         | Google Cloud Storage Bucket name which stores export-billing data   |
| dataset             | BigQuery Dataset name which will store export-billing data          |
| table_header        | Header for BigQuery table. Table name will be {table_header}_YYYYMM |
| project_id          | Your Google Platform Project ID                                     |
| report_header       | Google Cloud Storage Object header for export-billing data          |

You can deploy App Engine script as follows:

```
$ git clone https://github.com/noralife/gcp-export-billing-bq
$ cd gcp-export-billing-bq
$ # Skip fi you use existing dataset
$ bq mk --data_location US {dataset}
$ # Set ACL to access export-billing data from AppEngine
$ gsutil acl ch -u {project_name}@appspot.gserviceaccount.com:WRITE gs://{bucket_name}/
$ pip install -t ./lib -r requirements.txt
$ appcfg.py update . -A {project_name} -V 1 -E BUCKET_NAME:{bucket_name} -E DATASET_ID:{dataset} -E TABLE_HEADER:{table_header} -E PROJECT_ID:{project_id} -E REPORT_HEADER:{report_header}
$ appcfg.py set_default_version -A {project_name} -V 1 .
```

## Manual insert

Instead of auto insert, you can manually insert data in BigQuery as follows:

```
$ git clone https://github.com/noralife/gcp-export-billing-bq
$ cd gcp-export-billing-bq
$ gsutil cp gs://{bucket_name}/{export_billing_csv_filename}.csv .
$ pip install -r requirements.txt
$ python formatter.py {export_billing_csv_filename}.csv
$ bq mk --schema schema.json -t billing.{table_header}_YYYYMM
$ bq insert billing.{table_header}_YYYYMM {export_billing_csv_filename}.csv.json
```
