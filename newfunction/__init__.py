import logging
import azure.functions as func
from azure.storage.blob import BlockBlobClient
import os

ACCOUNT_NAME = os.environ['ACCOUNT_NAME']
SAS_TOKEN = os.environ['SAS_TOKEN']
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    file="" 
    fileContent=""       
    blob_service = BlockBlobClient(account_name=ACCOUNT_NAME,account_key=None,sas_token=SAS_TOKEN)
    containername= os.environ['CONTAINER_NAME']
    generator = blob_service.list_blobs(container_name=containername) #lists the blobs inside containers
    for blob in generator:
        file=blob_service.get_blob_to_text(containername,blob.name) 
        logging.info(file.content)
        fileContent+=blob.name+'\n'+file.content+'\n\n'

    return func.HttpResponse(f"{fileContent}")