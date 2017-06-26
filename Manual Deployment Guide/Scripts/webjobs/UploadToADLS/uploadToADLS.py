import sys, os

from azure.datalake.store import core, lib, multithread

cwd = os.getcwd()

adl_name = os.environ['DATALAKESTORE_NAME']
tenant_id = os.environ['TENANT_ID']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']

#localPath='D:\\home\\site\\wwwroot\\app_data\\jobs\\triggered\\uploadStaticData\\staticData\\'

dir_list = next(os.walk('.'+'\\staticData\\'))[1]
token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
adls_file_system_client = core.AzureDLFileSystem(token, store_name=adl_name)

for dir in dir_list:
    local_path=cwd+'\\staticData\\'+dir
    print(local_path)
    remote_path=dir+'\\'
    multithread.ADLUploader(adls_file_system_client, lpath=local_path , rpath= remote_path,overwrite=True)

