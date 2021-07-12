#!/usr/bin/env python3
__author__="paul kannan iyyanar"

import sys, traceback
from util.gcsutils import GCSUtils
from google.cloud import storage
from datetime import datetime
from util.fileutils import FileUtils
from util.sftp_push import SFTPpush
from util.generate_key import GenerateKey

class TransferAppl:
    def gcs_to_Datorama(self, _csv_gcs_bucket_name, _csv_gcs_directory, context):
        try:
            _localcsvFileLoc='tmp/outbound/'
            FileUtils.createLocalDirIfNotExists(_localcsvFileLoc)
            FileUtils.emptyDir(_localcsvFileLoc)
            currdate = datetime.now().strftime('%Y%m%d')
            localcsvfiles=[]
            pattern='{}{}/*'.format(_csv_gcs_directory, currdate)
            print('bucket search pattern: '+pattern)
            storage_client = storage.Client()
            fileList=GCSUtils.list_files(storage_client, _csv_gcs_bucket_name, pattern)
            context.logger.info('no of files in gcs {0}'.format(len(fileList)))
            if len(fileList) == 0:
                context.logger.info('no files in gcs bucket')
                return
            #Step1: Download CT from GCS bucket
            for file in fileList:
                fileStr=str(file)
                if fileStr.__contains__('/'):
                    filename= fileStr.split('/'[-1])
                else:
                    filename = fileStr

                context.logger.info("GCS download: {}".format(file))
                print("GCS download: {}".format(file))
                GCSUtils.download_blob(storage_client, _csv_gcs_bucket_name, file, _localcsvFileLoc+filename, context)
                localcsvfiles.append(filename)
            localcsvfiles.sort()
            #Step 3: Download SFTP encrypted keyfrom Gcs bucket and decrypt it
            datorama_key_bucket=context.settings.get('datorama_key_bukcet_name')
            sftp_key_file=context.settings.get('Sftp encrypted file')
            GCSUtils.download_blob(storage_client, datorama_key_bucket, sftp_key_file, '/tmp/sftp', context)
            context.logger.info('sftp key file downloaded ...')
            password=GenerateKey.get_sftp_key(context)

            #Step4 : SFTP push individual CT files to datorama
            for file in localcsvfiles:
                SFTPpush.sftp_push(context, _localcsvFileLoc+file, password)
                with open("/tmp/paramiko.log",'r') as file:
                    data=file.read()
                    context.logger.info("paramiko log: "+data)
        except Exception as E:
            print("Error in transfer application : {}".format(sys.exc_info()[1]))
            context.logger.error('Error: {}'.format(traceback.format_exc()))