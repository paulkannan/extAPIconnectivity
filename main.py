#!/usr/bin/env python3

__name__="Paul kannan Iyyanar"

import sys, traceback
from util.context import Context

from app.interactmgr import InteractionManager
from app.transferapp import TransferAppl

def execute_ingress():
    prefix='inbound'
    context=Context(prefix) #load settings from config.yml and create logger
    settings=context.settings
    _csv_gcs_bucket_name=settings.get('DA_CSV_GCS_BUCKET_NAME')
    _csv_gcs_directory= settings.get('DMA_CSV_GCS_INBOUND')
    endpoint=settings.get('da_endpoint')
    path= settings.get('da-bodypath')
    workspace_ids=settings.get('workspace_ids')
    context.logger.info('started {}'.format(prefix))
    try:
        print("Started Ingress")
        for id in workspace_ids.split(','):
            InteractionManager.getAPIData(endpoint, path, _csv_gcs_bucket_name, _csv_gcs_directory,id, context)
    except Exception as e:
        print ('error in inbound: {}'.format(sys.exc_info()[1]))

def execute_egress():
    prefix='outbound-process'
    context=Context(prefix)
    settings=context.settings
    _csv_gcs_bucket_name = settings.get('DA_CSV_GCS_BUCKET_NAME')
    _csv_gcs_directory = settings.get('DMA_CSV_GCS_outBOUND')
    context.logger.info('started {}'.format(prefix))
    try:
        print("Started egress")
        TransferAppl.gcs_to_Datorama(_csv_gcs_bucket_name, _csv_gcs_directory, context)
    except Exception as e:
        print("error in outbound: {}" .format(sys.exc_info()[1]))


if __name__ == '__main__':
    command = sys.argv[1]
    if command == "egress":
        execute_egress()
    elif command="ingress":
        execute_ingress()
    else:
        print('provide valid args')


