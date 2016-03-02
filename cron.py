import cloudstorage as gcs
import datetime
import formatter
import httplib2
import json
import logging
import os
import traceback
import uuid
import webapp2

from apiclient.discovery import build
from oauth2client.contrib.appengine import AppAssertionCredentials

SCOPE = 'https://www.googleapis.com/auth/bigquery'

my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)

gcs.set_default_retry_params(my_default_retry_params)


class CronJob(webapp2.RequestHandler):
    class Env(object):
        @classmethod
        def bucket_name(cls):
            return os.environ.get('BUCKET_NAME')

        @classmethod
        def project_id(cls):
            return os.environ.get('PROJECT_ID')

        @classmethod
        def dataset_id(cls):
            return os.environ.get('DATASET_ID')

        @classmethod
        def table_header(cls):
            return os.environ.get('TABLE_HEADER')

        @classmethod
        def report_header(cls):
            return os.environ.get('REPORT_HEADER')

    def get(self):
        self.response.headers['Content-Type'] = 'application/json'

        try:
            job_id = str(uuid.uuid4())
            bucket_name = self.Env.bucket_name()
            report_header = self.Env.report_header()
            date = datetime.datetime.now() + datetime.timedelta(days=-3)
            filename = '/' + bucket_name \
                       + '/' + report_header \
                       + '-' + date.strftime('%Y-%m-%d') + '.csv'

            logging.info(job_id + " cron job started")

            logging.info(job_id + " authenticating...")
            bigquery = self.auth_bq()

            table_id = self.Env.table_header() + '_' + date.strftime('%Y%m')
            logging.info(job_id + " checking table " + table_id + "...")
            if not self.exist_table(bigquery, table_id):
                logging.info(job_id + " creating table...")
                schema = self.read_schema('./schema.json')
                self.create_table(bigquery, table_id, schema)

            logging.info(job_id + " loading " + filename + "...")
            data = self.read_file(filename)

            logging.info(job_id + " inserting data...")
            self.insert_data(bigquery, table_id, data)

            obj = {'status': 'success'}
            self.request.set_status = 200
            logging.info(job_id + " cron job finished")
        except Exception as err:
            obj = {'status': 'error', 'message': repr(err)}
            self.error(500)
            logging.error(job_id + " cron job aborted")
            logging.error(traceback.format_exc())
        finally:
            self.response.write(json.dumps(obj))

    def exist_table(self, bigquery, table_id):
        res = bigquery.tables().list(projectId=self.Env.project_id(),
                                     datasetId=self.Env.dataset_id()).execute()
        logging.info(res)
        if 'tables' in res:
            for table in res['tables']:
                if table_id == str(table['tableReference']['tableId']):
                    return True

        return False

    def create_table(self, bigquery, table_id, schema):
        body = {
            'schema': {'fields': schema},
            'tableReference': {
                'projectId': self.Env.project_id(),
                'tableId': table_id,
                'datasetId': self.Env.dataset_id()
            }
        }
        res = bigquery.tables().insert(projectId=self.Env.project_id(),
                                       datasetId=self.Env.dataset_id(),
                                       body=body).execute()
        logging.info(res)
        return True

    def insert_data(self, bigquery, table_id, data):
        rows = []
        for index, raw_row in enumerate(data):
            row = {}
            row['insertId'] = index
            row['json'] = raw_row
            rows.append(row)

        body = {'rows': rows}
        res = bigquery.tabledata().insertAll(projectId=self.Env.project_id(),
                                             datasetId=self.Env.dataset_id(),
                                             tableId=table_id,
                                             body=body).execute()
        logging.info(res)
        return True

    def auth_bq(self):
        credentials = AppAssertionCredentials(scope=SCOPE)
        http = credentials.authorize(httplib2.Http())
        bigquery = build('bigquery', 'v2', http=http)
        return bigquery

    def read_file(self, filename):
        gcs_file = gcs.open(filename)
        data = gcs_file.read()
        json = formatter.load_export_billing_csv_from_memory(data)
        formatted_json = formatter.format_rows(json)
        gcs_file.close()
        return formatted_json

    def read_schema(self, filename):
        with open(filename) as data_file:
            data = json.load(data_file)
        return data


app = webapp2.WSGIApplication([('/cron', CronJob)], debug=True)
