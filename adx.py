from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder 
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.keyvault.secrets import SecretClient
import pandas as pd
import os

os.environ['key_vault_url'] = "https://nebaridia.vault.azure.net/"
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=os.environ.get('key_vault_url'), credential=credential)

def get_kusto_client(cluster=secret_client.get_secret('cluster-adx').value ):
    credential = DefaultAzureCredential()
    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster, credential)
    client = KustoClient(kcsb)
    return client

def perform_query(query, table = "Volve" ,   database = "test",  client = get_kusto_client()  ) -> pd.DataFrame: 
    """Perform any kustoQueryLanguage (kql) query to the adx database that contains time series data and return it as pd.DataFrame.
    Args: 
        query (str): The kql query to perform. Example query to get 10 rows from the Volve table: f"{table} | take 10" 
    Returns: 
        (pd.DataFrame): The pandas dataframe of the results of the query
    """
    try:
        response = client.execute(database, query)
        return dataframe_from_result_table(response.primary_results[0])
    except Exception as e:
        print(f"Query failed with error: {str(e)}")