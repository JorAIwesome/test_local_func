import logging
import azure.functions as func
import pandas as pd
import io
import os
from azure.storage.blob import BlobServiceClient

def access_file_from_adls_with_sas(account_name, sas_token, container_name, folder_name, file_name):
    try:
        blob_url = f"https://{account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=blob_url, credential=sas_token)

        blob_path = file_name if folder_name in ['', '.'] else f"{folder_name}/{file_name}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        if not blob_client.exists():
            logging.error(f"Blob '{blob_path}' bestaat niet.")
            return None

        download_stream = blob_client.download_blob()
        file_stream = io.BytesIO(download_stream.readall())

        if file_name.endswith('.xlsx'):
            df = pd.read_excel(file_stream)
        elif file_name.endswith('.csv'):
            df = pd.read_csv(file_stream, sep=';')
        else:
            raise ValueError("Onbekend bestandstype")

        return df

    except Exception as e:
        logging.error(f"Fout bij ophalen blob: {e}")
        return None


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Function trigger ontvangen.")

    # 🔐 Gebruik env vars (Application Settings in Azure)
    account_name = os.getenv('ACCOUNT_NAME')
    sas_token = os.getenv('SAS_TOKEN')
    container_name = os.getenv('CONTAINER_NAME')
    folder_name = '.'  # of iets als 'submap' indien van toepassing
    file_name = 'Roosterdata_HHH_8.xlsx'

    if not all([account_name, sas_token, container_name]):
        return func.HttpResponse("Missing environment variables", status_code=500)

    df = access_file_from_adls_with_sas(account_name, sas_token, container_name, folder_name, file_name)

    if df is None:
        return func.HttpResponse("Fout bij ophalen of lezen van het bestand", status_code=500)

    eerste_waarde = df.iloc[1, 0]
    return func.HttpResponse(f"Eerste waarde uit Excel-bestand: {eerste_waarde}", status_code=200)
