#This script downloads the inventory optimization scripts from Azure Data Lake Store to specified local directory

from azure.datalake.store import core, lib, multithread
import os

_ADL_NAME = os.environ['DATALAKESTORE_NAME']
_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']

if __name__ == '__main__':

    scripts_local_path =  './inventory_scripts'
    scripts_adl_path = '/inventory_scripts'

    adl_token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
    adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=_ADL_NAME)

    multithread.ADLUploader(adls_file_system_client, lpath=scripts_local_path , rpath= scripts_adl_path,overwrite=True)   
