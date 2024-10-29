import logging
import azure.functions as func
import requests
import base64
import pandas as pd
from io import StringIO, BytesIO
from azure.storage.blob import BlobServiceClient
import os
import time
from datetime import date

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
        
        blob_name = "defi-tracker.csv" 
        existing_data = retrieve_csv_from_blob(blob_name)
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

        save_csv_to_blob(updated_df,blob_name)

    except requests.RequestException as e:
        print(f"Request error: {e}")

def retrieve_csv_from_blob(blob_name):
    try:
        
        container_name = "defi-tracker"

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

def save_csv_to_blob(df, blob_name):
    try:

        container_name = "defi-tracker"

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

@app.timer_trigger(schedule="0 0 12 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def CheckAPYs(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    url = 'https://public-api.de.fi/graphql/'  # Replace this with the actual De.Fi GraphQL endpoint
    try:

        chainsQuery = """
        query {
            chains {
                id
                absoluteChainId
                abbr
                name
                type

                }
            }
        """
        # Define the GraphQL query
        query = """
        {
        opportunities {
            id
            chainId
            apr
            totalValueLocked
            categories
            investmentUrl
            isNew
            status
            farm {
            id
            url
            slug
            logo
            categories
            }
            tokens {
            borrowRewards {
                address
                displayName
                icon
                symbol
                name
            }
            deposits {
                address
                displayName
                icon
                symbol
                name
            }
            rewards {
                address
                displayName
                icon
                symbol
                name
            }
            }
        }
        }

        """
        
        # Set up the request headers
        headers = {
            'Content-Type': 'application/json',
            'X-Api-Key': os.environ['API_KEY_DEFI']  # Replace with your actual API key if required
        }
        
        responseChains = requests.post(
            url,
            json={'query': chainsQuery},
            headers=headers
        )
        
        chainsDict = {}
        dataChains = responseChains.json()
        for chain in dataChains['data']['chains']:
            chainsDict[chain['id']] = chain['name']
        
        time.sleep(10)
        
        # Make the request
        response = requests.post(
            url,
            json={'query': query},
            headers=headers
        )
        
        blob_name = "APY-tracker.csv" 
        existing_data = retrieve_csv_from_blob(blob_name)
        data = response.json()
        
        new_data = {
            'Defi Protocol': [],
            'Network': [],
            'Updated At': [],
            'Token': [],
            'APY': []
        }
        opportunities = data['data']['opportunities']
        for opportunity in opportunities:
            if (any(s in opportunity['categories'] for s in ["lending", "stablecoin"])):
                new_data['Network'].append(chainsDict[int(opportunity['chainId'])])
                new_data['APY'].append(opportunity['apr'] * 100)
                new_data['Defi Protocol'].append(opportunity['farm']['slug'])
                new_data['Token'].append(opportunity['tokens']['deposits'][0]['displayName'])
                new_data['Updated At'].append(date.today())
        
        new_df = pd.DataFrame(new_data)
        
        if existing_data is not None:
            updated_df = pd.concat([existing_data, new_df], ignore_index=True)
        else:

            updated_df = new_df
        
        save_csv_to_blob(updated_df,blob_name)
    
    except requests.RequestException as e:
        print(f"Request error: {e}")