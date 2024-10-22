import logging
import azure.functions as func
import requests
import base64
import pandas as pd
from io import StringIO, BytesIO
from azure.storage.blob import BlobServiceClient
import os
import time

app = func.FunctionApp()

@app.schedule(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def CheckYields(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    #https://studio.zapper.xyz/documentation#tag/Balances
    api_key = os.environ['API_KEY']
    address= os.environ['ADDRESS']
    network= "Ethereum"


    # Create the encoded credentials once
    encoded_credentials = base64.b64encode(f"{api_key}:".encode()).decode("utf-8")
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "accept": "/"
    }

    get_token_balances(headers,address,network)

def get_token_balances(headers,address,network):
    url = f"https://api.zapper.xyz/v2/balances/apps?addresses%5B%5D={address}&network={network}"
    try:
        
        responsePost = requests.post(url, headers=headers)
        responsePost.raise_for_status()

        time.sleep(10)
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        existing_data = retrieve_csv_from_blob()
        data = response.json()
        
        new_data = {
            'App Name': [],
            'Network': [],
            'Updated At': [],
            'Token': [],
            'Balance': [],
            'USD value': []
        }
        for token in data:
            new_data['App Name'].append(token['appName'])
            new_data['Network'].append(token['network'])
            new_data['Updated At'].append(token['updatedAt'])
            new_data['Token'].append(token['products'][0]['assets'][0]['tokens'][0]['symbol'])
            new_data['Balance'].append(token['products'][0]['assets'][0]['tokens'][0]['balance'])
            new_data['USD value'].append(token['balanceUSD'])
        
        new_df = pd.DataFrame(new_data)
        
        if existing_data is not None:
            updated_df = pd.concat([existing_data, new_df], ignore_index=True)
        else:

            updated_df = new_df

        save_csv_to_blob(updated_df)

    except requests.RequestException as e:
        print(f"Request error: {e}")

def retrieve_csv_from_blob():
    try:
        
        container_name = "defi-tracker"
        blob_name = "defi-tracker.csv"  

        blob_service_client = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage'])
        container_client = blob_service_client.get_container_client(container_name)

        # Retrieve the blob (CSV file)
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()

        # Load the CSV data into a pandas DataFrame
        csv_data = StringIO(blob_data.decode('utf-8'))
        df = pd.read_csv(csv_data)

        logging.info(f"Retrieved existing CSV file: {blob_name}")
        return df

    except Exception as e:
        logging.error(f"Error retrieving file from Blob Storage: {e}")
        return None

def save_csv_to_blob(df):
    try:

        container_name = "defi-tracker"
        blob_name = "defi-tracker.csv" 

        # Create a BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(os.environ['AzureWebJobsStorage'])
        container_client = blob_service_client.get_container_client(container_name)

        # Convert the DataFrame to CSV and write it to Blob Storage
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(csv_buffer.getvalue(), blob_type="BlockBlob", overwrite=True)

        logging.info(f"File uploaded to Azure Storage: {blob_name}")

    except Exception as e:
        logging.error(f"Error uploading file to Blob Storage: {e}")