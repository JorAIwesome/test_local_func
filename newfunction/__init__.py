import azure.functions as func
import pandas as pd
import requests
import io

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        file_url = "https://blobhhh.blob.core.windows.net/containerhhh/Roosterdata_HHH_8.xlsx"

        # Bestand ophalen via publieke URL
        response = requests.get(file_url)
        response.raise_for_status()  # Fout afvangen als bestand niet bestaat

        # Lees de Excel-inhoud in een pandas DataFrame
        file_stream = io.BytesIO(response.content)
        df = pd.read_excel(file_stream)

        # Voorbeeld: geef eerste cel terug als controle
        out = df.iloc[0, 0]

        return func.HttpResponse(f"Eerste cel in het bestand is: {out}", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"Fout bij ophalen of lezen van bestand: {e}", status_code=500)
