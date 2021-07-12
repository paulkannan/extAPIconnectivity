#!/usr/bin/env python3
__author__ = "Paul Kannan Iyyanar"

import os, sys, csv
from util.gcsutils import GCSUtils
from google.cloud import storage
from datetime import datetime
from util.fileutils import FileUtils
import json, traceback, requests, re
from util.generate_key import GenerateKey
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest

class InteractionManager:
    def Download_API_body(self, context):
        try:
            #Step 1 : Download API body file from GCS bucket
            config_path = context.settings.get("API_body_path")
            gcs_bucket_name = context.settings.get("dato_csv_gcs_bucket_name")
            storage_client=storage.Client()
            _localcsvfileloc="./config/body.txt"
            print("Download Started")
            context.logger.info("Downloading config file")
            GCSUtils.download_blob(storage_client, gcs_bucket_name, config_path, _localcsvfileloc, context)
            print("Download completed")
            context.logger.info('Downloaded config'.format(gcs_bucket_name+config_path)

        except Exception as e:
            print('Error in  Inbound Process'. format(sys.exc_info()[1]))
            context.logger.error('Error in Inbound Process: {}'.format(sys.exc_info[1]))
            context.logger.error('Error in Inbound Process: {}'.format(traceback.format_exc()))
            sys.exit()

    def getAPIData(self, endpoint, path, csv_gcs_bucket_name, _csv_gcs_directory, workspaceid, context):
        try:
            proxy=context.settings.get('PROXY')
            proxies={"https": proxy,
                     "http": proxy
                     }
            #Get API data from datorama
            context.logger.info("Started fetching data from API")
            body=FileUtils.getBody(path)
            body.replace("<workspace_id>", workspaceid)

            #Step2: Download encrypted API token to make API call
            datorama_key_bucket= context.settings.get('datorama_key_bucket_name')
            api_key_file=context.settings.get('api_encrypted_file')
            storae_client=storage.Client()
            GCSUtils.download_blob(storage_client, gcs_bucket_name, config_path, _localcsvfileloc, context)
            context.logger.info("Downloading apr file")

            #Step3: Decrypt encrypted API token to make api call
            token=GenerateKey.get_API_key(context)
            header={'token': token, 'Content-Type': 'application/json; charset=UTF-8','Accept': 'application/json'}
            query=json.loads(body)
            print("Making API query")

            #Step4: Make API request and fetch the data
            data=request.post(endpoint, headers=header, json=query, proxies=proxies)

            if data.status_code != requests.codes.ok:
                context.logger.info("API response...."+str(data.status_code))
                context.logger.info('API response ...'+str(data.json()))
                raise Exception("Api fetch failed")
            data_json=data.json()

            #Step5: Verify for errors
            if len(data_json[errors])!=0:
                context.logger.error("Error in received API data {}"+"\n".join(data_json['errors']))
                print(data_json['errors'])
                raise Exception("Errors in API data")
            context.logger.info("End fetching data")
            
            local_directory='tmp/ingress/'
            currdate=datetime.now().strftime("%Y%m%d")

            FileUtils.createLocalDirIfNotExists(local_directory)

            input_file=local_directory+'datorama_'+currdate+'.json'

            #Step6: save json response to NEW line delimited json
            context.logger.info("Start preparing Json file")
            header=data_json['queryResponseData']['headers']
            header_underscores=re.sub(' +', '_', re.sub('[^A-Za-z0-9_ |]+', '', "|".join(header).replace("%","PCT"))).split("|")
            context.logger.info("Headers: "+", ".join(header_underscores))
            file=open(input_file, 'w+' , newline='')

            result=[dict(zip(header_underscores, entry)) for entry in data_json['queryResponseData']['rows']]
            for line in result:
                file.write(json.dumps(line))
                file.write('\n')
            file.close()

            context.logger.info("End preparing json file")

            #Step7: Upload new line delimited Json file to gcs

            GCSUtils.upload_blob(storage_client, _csv_gcs_bucket_name, input_file,_csv_gcs_directory+'datorama_'+ currdate +'.csv', context)
            _JSONGCSfileURI = 'gs://'+csv_gcs_bucket_name+'/'+_csv_gcs_directory+'drm_'+currdate+'.json'
            client=bigquery.Client()
            _bq_table=context.settings.get('INbound_bq_table')
            _bq_dataset_location=context.settings.get('bq_dataset_location')
            dbdetails=_bq_table.split('.')
            dataset_id=dbdetails[-2]
            TABLE_NAME = dbdetails[-1]
            project=context.settings.get('CDL_Project')
            dataset_ref=client.dataset(dataset_id, project)

            #Step8: Load data from GCS to BQ
            job_config=bigquery.LoadJobConfig()
            print("json uri"+_JSONGCSfileURI)
            context.logger.info("json uri"+_JSONGCSfileURI)
            job_config.source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            statinfo = os.stat(input_file)
            if statinfo.st_size>0:
                print("Json file loading to GQ")
                context.logger.info('json file loading to bq table {}'.format(_bq_table))
                load_job= client.load_table_from_uri(
                    _JSONGCSfileURI,
                    dataset_ref.table(TABLE_NAME),
                    location=_bq_dataset_location,
                    job_config=job_config
                )
                load_job.result()
                print("BQ load status: {}, Target table: {}".format(load_job.state, TABLE_NAME))

            else:
                print("Can't load json file bcos file size is zero")

        except BadRequest as E:
            if load_job is not None:
                for e in load_job.errors:
                    context.logger.error('Error in bq load data: {}'.format(e['message']))
        except Exception as E:
            print('Error in intermgr: {}'.format(sys.exc_info()[1]))
            context.logger.error("Error: {}".format(traceback.format_exc()))



            