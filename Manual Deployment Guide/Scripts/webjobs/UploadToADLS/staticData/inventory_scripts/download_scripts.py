#This script downloads the inventory optimization scripts from Azure Data Lake Store to specified local directory

from azure.datalake.store import core, lib, multithread
import argparse

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--scripts_adl_dir', required=True)
    parser.add_argument('--policy_adl_subdir', required=True)
    parser.add_argument('--policy_script', required=True)
    parser.add_argument('--adl_name', required=True)
    parser.add_argument('--adl_tenant_id', required=True)
    parser.add_argument('--adl_client_id', required=True)
    parser.add_argument('--adl_client_secret', required=True)
    
    args = parser.parse_args()

    scripts_adl_dir =  args.scripts_adl_dir
    policy_adl_subdir = args.policy_adl_subdir
    policy_script = args.policy_script

    adl_name = args.adl_name
    tenant_id = args.adl_tenant_id
    client_id = args.adl_client_id
    client_secret = args.adl_client_secret
    
    token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    adls_file_system_client = core.AzureDLFileSystem(token, store_name=adl_name)
    
    
    task_script= scripts_adl_dir +'/inventory_optimization_task.py'
    upload_script = scripts_adl_dir +'/upload_to_adls.py'
    mipcl_script = scripts_adl_dir +'/mipcl_wrapper.py'
    policy_script = scripts_adl_dir + '/' + policy_adl_subdir + '/' + policy_script

    file_list = [task_script,upload_script,mipcl_script,policy_script]

    scripts_local_path =  '/taskscripts/'

    for file in file_list:
        multithread.ADLDownloader(adls_file_system_client, lpath=scripts_local_path , rpath= file,overwrite=True)   
