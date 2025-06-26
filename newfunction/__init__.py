import logging
import azure.functions as func
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
import os

def access_file_from_adls_with_sas(account_name, sas_token, container_name, folder_name, file_name):
    try:
        # Maak blob service URL zonder container of bestand
        blob_url = f"https://{account_name}.blob.core.windows.net"

        # Initialiseer de client met de SAS-token
        blob_service_client = BlobServiceClient(account_url=blob_url, credential=sas_token)

        # Bestandsnaam inclusief folder (als folder_name is '.', dan alleen file_name)
        blob_path = file_name if folder_name in ['', '.'] else f"{folder_name}/{file_name}"

        # Haal de blob client op
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        if not blob_client.exists():
            logging.error(f"Blob '{blob_path}' bestaat niet.")
            return None

        # Download inhoud als bytes
        download_stream = blob_client.download_blob()
        file_content = download_stream.readall()

        # Lees bestand als DataFrame
        file_stream = io.BytesIO(file_content)

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

    # Je SAS-configuratie (bij voorkeur uit app settings via os.getenv)
    account_name = os.getenv('ACCOUNT_NAME')
    sas_token = os.getenv('SAS_tok')
    container_name = os.getenv('CONTAINER_NAME')
    folder_name = ""  # of "" als er geen subfolder is
    file_name = "Roosterdata_HHH_8.xlsx"

    df = access_file_from_adls_with_sas(account_name, sas_token, container_name, folder_name, file_name)

    if df is None:
        return func.HttpResponse("Kon bestand niet openen of lezen.", status_code=500)

    # Bijv. eerste celwaarde teruggeven
    eerste_waarde = df.iloc[1, 0]
    return func.HttpResponse(f"Eerste waarde in het bestand: {eerste_waarde}")
