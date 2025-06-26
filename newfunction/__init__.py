import azure.functions as func
import pandas as pd
import io
import requests
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        base_url = "https://blobhhh.blob.core.windows.net/containerhhh/Roosterdata_HHH_8.xlsx"
        sas_token = os.getenv('SAS_tok')

        full_url = f"{base_url}?{sas_token}"

        # Haal de blob op
        response = requests.get(full_url)
        response.raise_for_status()

        # Lees de Excel in als DataFrame
        file_stream = io.BytesIO(response.content)
        df = pd.read_excel(file_stream)

        eerste_cel = df.iloc[1, 0]

        return func.HttpResponse(f"Eerste cel: {eerste_cel}", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Fout bij ophalen of verwerken blob: {e}", status_code=500)
