#
# Copyright © Microsoft Corporation (“Microsoft”).
#
# Microsoft grants you the right to use this software in accordance with your subscription agreement, if any, to use software 
# provided for use with Microsoft Azure (“Subscription Agreement”).  All software is licensed, not sold.  
# 
# If you do not have a Subscription Agreement, or at your option if you so choose, Microsoft grants you a nonexclusive, perpetual, 
# royalty-free right to use and modify this software solely for your internal business purposes in connection with Microsoft Azure 
# and other Microsoft products, including but not limited to, Microsoft R Open, Microsoft R Server, and Microsoft SQL Server.  
# 
# Unless otherwise stated in your Subscription Agreement, the following applies.  THIS SOFTWARE IS PROVIDED “AS IS” WITHOUT 
# WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL MICROSOFT OR ITS LICENSORS BE LIABLE 
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THE SAMPLE CODE, EVEN IF ADVISED OF THE 
# POSSIBILITY OF SUCH DAMAGE.
#

from __future__ import print_function
import datetime, time, os, sys, getopt
import pandas as pd
import json,cronex,uuid,subprocess

#from pyomo.environ import *

from azure.datalake.store import core, lib, multithread
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

from azure.mgmt.datalake.analytics.job import DataLakeAnalyticsJobManagementClient
from azure.mgmt.datalake.analytics.job.models import JobInformation, JobState, USqlJobProperties

from azure.common.credentials import ServicePrincipalCredentials

import invutils as utils

now = datetime.datetime.now()
current_date = now.date()
current_datetime_tuple = (now.year,now.month,now.day,now.hour,0)
current_datetime_string = datetime.datetime.strftime(now,"%Y%m%d%H%M")

_SUBSCRIPTION_ID = os.environ['SUBSCRIPTION_ID']
_RESOURCE_GROUP = os.environ['RESOURCE_GROUP']

_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']

if 'DOCKER_REGISTRY_SERVER' in os.environ:
    docker_registry_flag = True
    _DOCKER_REGISTRY_SERVER = os.environ['DOCKER_REGISTRY_SERVER']
    _DOCKER_USERNAME = os.environ['DOCKER_REGISTRY_USERNAME']
    _DOCKER_PASSWORD = os.environ['DOCKER_REGISTRY_PASSWORD']
else:
    docker_registry_flag = False

if 'VIRTUAL_NETWORK_NAME' in os.environ:
    virtual_network_flag = True
    _VIRTUAL_NETWORK_NAME = os.environ['VIRTUAL_NETWORK_NAME']
    _VIRTUAL_NETWORK_RESOURCE_GROUP = os.environ['VIRTUAL_NETWORK_RESOURCE_GROUP']
    _VIRTUAL_NETWORK_SUBNET_NAME = os.environ['VIRTUAL_NETWORK_SUBNET_NAME']
    _VIRTUAL_NETWORK_ADDRESS_PREFIX = os.environ['VIRTUAL_NETWORK_ADDRESS_PREFIX']
else:
    virtual_network_flag = False

#Batch account credential strings
_BATCH_ACCOUNT_NAME = os.environ['BATCH_ACCT_NAME']
_BATCH_ACCOUNT_URL = os.environ['BATCH_ACCT_URL']

# Storage account credential strings
_STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCT']
_STORAGE_ACCOUNT_KEY = os.environ['STORAGE_KEY']
_STORAGE_ACCOUNT_ENDPOINT = os.environ['STORAGE_ENDPOINT']

#pool configurations
_POOL_NODE_COUNT = int(os.environ['BATCH_START_NODE_COUNT'])
_MAX_TASKS_PER_NODE = int(os.environ['BATCH_MAX_TASK_PER_NODE'])
_POOL_VM_SIZE = os.environ['BATCH_VM_SIZE']
_NODE_OS_PUBLISHER = os.environ['BATCH_NODE_OS_PUBLISHER']
_NODE_OS_OFFER = os.environ['BATCH_NODE_OS_OFFER']
_NODE_OS_SKU = os.environ['BATCH_NODE_OS_SKU']

# ADL configurations 
_ADL_NAME = os.environ['DATALAKESTORE_NAME']
au_per_usql_job = int(os.environ['ADLA_AU_PER_USQL_JOB'])  # Analytic Units per USQL script execution

_CONTAINER_NAME = os.environ['DOCKER_REGISTRY_IMAGE']

#other configurations
_POOL_ID = 'InvOpt'+ current_datetime_string
_JOB_ID = 'InventoryOptimization'

def create_configuration_files(config_dir):
    #credentials dictionary
    credentials = {
        "credentials": {
            "batch": {
                "account_service_url": _BATCH_ACCOUNT_URL,
                "aad": {
                    "endpoint": "https://batch.core.windows.net/",
                    "directory_id": _TENANT_ID ,
                    "application_id": _CLIENT_ID,
                    "auth_key": _CLIENT_SECRET
            },
                "resource_group": _RESOURCE_GROUP
            },
            "storage": {
                _STORAGE_ACCOUNT_NAME : {
                        "account": _STORAGE_ACCOUNT_NAME,
                        "account_key": _STORAGE_ACCOUNT_KEY,
                        "endpoint": _STORAGE_ACCOUNT_ENDPOINT
                }
            },
            "management": {
                "subscription_id": _SUBSCRIPTION_ID,
                "aad": {
                    "endpoint": "https://management.core.windows.net/",
                    "directory_id": _TENANT_ID ,
                    "application_id": _CLIENT_ID,
                    "auth_key": _CLIENT_SECRET
                    }
                }  
            }
        }
    
    #batch shipyard configuration dictionary
    config = {
        "batch_shipyard": {
            "storage_account_settings":  _STORAGE_ACCOUNT_NAME
        },
        "global_resources": {
            "docker_images": [
                _CONTAINER_NAME
            ]
        }
    }

    #pool configuration dictonary
    pool={
        "pool_specification": {
            "id": _POOL_ID,
            "vm_size":_POOL_VM_SIZE,
            #"vm_count":_POOL_NODE_COUNT,
            "vm_count":{
                "dedicated": _POOL_NODE_COUNT,
                "low_priority": 0
                },
            "max_tasks_per_node": _MAX_TASKS_PER_NODE,
            "publisher": _NODE_OS_PUBLISHER,
            "offer": _NODE_OS_OFFER,
            "sku": _NODE_OS_SKU,
            "ssh": {
                "username": "docker"
            },
            "reboot_on_start_task_failed": True,
            "block_until_all_global_resources_loaded": True
        }
    }

    #add optional docker registry configuration
    if docker_registry_flag:
        docker_registry_config =  {
            "private": {
                "allow_public_docker_hub_pull_on_missing": False,
                "server": _DOCKER_REGISTRY_SERVER
            }
        }
        docker_registry_credentials = {
            _DOCKER_REGISTRY_SERVER : {
                "username": _DOCKER_USERNAME,
                "password": _DOCKER_PASSWORD
                }
            }
        config["docker_registry"] = docker_registry_config
        credentials["credentials"]["docker_registry"] = docker_registry_credentials
    
    #add optional virtual network configuration
    if virtual_network_flag:
        virtual_network =  {
            "name": _VIRTUAL_NETWORK_NAME,
            "resource_group": _VIRTUAL_NETWORK_RESOURCE_GROUP,
            "create_nonexistant": False,
            "subnet": {
                "name": _VIRTUAL_NETWORK_SUBNET_NAME,
                "address_prefix": _VIRTUAL_NETWORK_ADDRESS_PREFIX
                }
            }
        pool["pool_specification"]["virtual_network"] = virtual_network

    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
    utils.write_json_to_file(credentials, os.path.join(config_dir, 'credentials.json'))
    utils.write_json_to_file(config, os.path.join(config_dir, 'config.json'))
    utils.write_json_to_file(pool, os.path.join(config_dir, 'pool.json'))


if __name__ == '__main__':
####################################################################################################################################
##Some preparation steps
    #simulation_datetime = "3/18/2017 16:45:23"
    opts,args = getopt.getopt(sys.argv[1:],"d:",["datetime="])

    simulation_datetime = None

    for opt, arg in opts:
      if opt in ("-d","--datetime"):
          simulation_datetime=arg

    if simulation_datetime:
        print(simulation_datetime)
        now = datetime.datetime.strptime(simulation_datetime,'%m/%d/%Y %H:%M:%S')
    #else:
    #    now = datetime.datetime.now()
        current_date = now.date()
        current_datetime_tuple = (now.year,now.month,now.day,now.hour,0)
        current_datetime_string = datetime.datetime.strftime(now,"%Y%m%d%H%M")

    f = open('datetimestring.txt','w')
    f.writelines(current_datetime_string)
    f.close()

    #ADLS directories and file names
    scripts_adl_folder = '/inventory_scripts'
    input_adl_folder = '/optimization/input_csv'
    output_adl_folder = '/optimization/output_csv'
    log_adl_folder = '/optimization/log'
    configuration_adl_folder = '/configuration'
    configuration_file_name = 'Configurations.xlsx'
    configuration_adl_path = configuration_adl_folder + '/' + configuration_file_name

    #local directories and file names
    optimization_config_path = 'config_optimization'
    scripts_local_path = os.path.realpath('./taskscripts')

    # Create the ADLS client
    adl_token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
    adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=_ADL_NAME)

    #Download configuration file from Azure Data Lake Store to local
    multithread.ADLDownloader(adls_file_system_client, lpath='.', rpath=configuration_adl_path, overwrite=True)
        
    #Read downloaded configuration file
    configuration_file_path = os.path.realpath(os.path.join('./', configuration_file_name))
    policy_all = pd.read_excel(configuration_file_path, 'InventoryPolicyConfig')
    solvers_all = pd.read_excel(configuration_file_path, 'SolverConfig')
    schedule_all = pd.read_excel(configuration_file_path, 'ScheduleConfig')

    policy_all = policy_all[policy_all['ActiveFlag'] == 1]

    #find policies to run in the current period
    policy_schedules_all = pd.merge(policy_all,schedule_all, left_on = 'ScheduleID_Optimization', right_on = 'ScheduleID')
    policy_schedules_all['TriggerFlag'] = policy_schedules_all.apply(utils.check_job_trigger,1,args=(current_datetime_tuple,current_date))
    active_policies = policy_schedules_all[policy_schedules_all['TriggerFlag'] == 1]
    active_policies_solvers = pd.merge(active_policies, solvers_all, on = 'SolverName')

    num_active_policies = active_policies_solvers.shape[0]
    
    if num_active_policies > 0:
        usql_timeout_all = active_policies_solvers['USQLTimeout']
        usql_timeout_max = int(sum(usql_timeout_all))

    ####################################################################################################################################
    ## Run usql queries to convert raw data to csv files for optimization problems

        policy_usql_job_list =  []

        for iP in range(num_active_policies):
            current_policy_solver = active_policies_solvers.iloc[iP]
            inventory_policy_name = current_policy_solver['InventoryPolicyName']
            directory_name = current_policy_solver['DirectoryName']
            usql_create_csv = current_policy_solver['USQLCreateCSV']
        
            usql_csv_files = usql_create_csv.split(',')
            script_cur = usql_csv_files[0]
            policy_usql_job_cur = utils.policy_usql_job(inventory_policy_name,directory_name,usql_csv_files)
            job_id_cur = utils.add_usql_job(scripts_adl_folder, directory_name, script_cur, adl_token,_ADL_NAME,simulation_datetime,au_per_usql_job)

            policy_usql_job_cur.update_job(job_id_cur, script_cur)

            print('Adding inventory policy '+ policy_usql_job_cur.policy + ': '+ policy_usql_job_cur.script_cur + '.usql to job queue.')

            policy_usql_job_list.append(policy_usql_job_cur)

        adla_job_client = DataLakeAnalyticsJobManagementClient(adl_token,  'azuredatalakeanalytics.net')
        timeout_expiration = datetime.datetime.now() + datetime.timedelta(minutes=usql_timeout_max)
        failed_usql_job_list = []
        successful_usql_job_list = []   
        while policy_usql_job_list and (datetime.datetime.now() < timeout_expiration):
            adl_token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
            adla_job_client = DataLakeAnalyticsJobManagementClient(adl_token,  'azuredatalakeanalytics.net')
            pending_usql_job_list = []
            for policy_job in policy_usql_job_list:
                jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                if jobResult.state==JobState.ended:
                    if jobResult.result.value=='Succeeded':
                        print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql finished successfully!')
                        policy_job.finish_job()
                        if policy_job.check_job_remain():
                            job_id_cur = utils.add_usql_job(scripts_adl_folder, policy_job.directory,policy_job.script_cur,adl_token,_ADL_NAME,simulation_datetime,au_per_usql_job)
                            policy_job.update_job_id(job_id_cur)
                            policy_job.reset_retry_left()
                            pending_usql_job_list.append(policy_job)
                            print('Adding inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql to job queue.')
                        else:
                            successful_usql_job_list.append(policy_job)
                            print('All jobs of inventory policy '+ policy_job.policy + ' finished successfully!')
                    else:
                        retry_left = policy_job.check_retry_left()
                        if retry_left > 0:
                            job_id_cur = utils.add_usql_job(scripts_adl_folder, policy_job.directory,policy_job.script_cur,adl_token,_ADL_NAME,simulation_datetime,au_per_usql_job)
                            policy_job.update_job_id(job_id_cur)
                            policy_job.reduce_retry_left()
                            pending_usql_job_list.append(policy_job)
                            print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql failed. Retrying... '+ str(policy_job.retry_left) + ' more retry(s) left.')
                        else: 
                            policy_job.mark_failed()
                            failed_usql_job_list.append(policy_job)
                else:
                    pending_usql_job_list.append(policy_job)
                    print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur +'.usql is not yet done. Current state: ' + jobResult.state.value)
            policy_usql_job_list = pending_usql_job_list
            time.sleep(10)
            print('Waiting for 10 seconds')

        inactivate_policy_list = []
        if policy_usql_job_list or failed_usql_job_list:
            if policy_usql_job_list:
                print('The following inventory policy job(s) timed out. Cancelling timeout jobs and no optimization tasks will be executed for these policies.')
                for policy_job in policy_usql_job_list:
                    jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                    if jobResult.state != JobState.ended:
                       inactivate_policy_list.append(policy_job.policy)
                       adla_job_client.job.cancel(_ADL_NAME, policy_job.job_id_cur)
                       jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                
                    print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql current state: '+ jobResult.result.value) 
            if failed_usql_job_list:
                print('The following inventory policy job(s) failed. No optimization tasks will be executed for these policies.')
                for policy_job in failed_usql_job_list:
                    inactivate_policy_list.append(policy_job.policy)
                    jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                    print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql current state: '+ jobResult.result.value)
        else:
            print('All inventory policy USQL jobs finished successfully')
    
    ####################################################################################################################################
    ## Solve optimization problems
        #remove policy from active policy list if USQL job failed
        if inactivate_policy_list:
            active_policies_solvers = active_policies_solvers[~active_policies_solvers['InventoryPolicyName'].isin(inactivate_policy_list)]

        num_active_policies = active_policies_solvers.shape[0]
        if num_active_policies > 0:
            optimization_timeout_all = active_policies_solvers['OptimizationTimeout']
            optimization_timeout_max = int(sum(optimization_timeout_all))
            #refresh ADLS token and client
            adl_token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
            adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=_ADL_NAME)
            #Create a list of configuration dictionaries for all inventory policies/jobs
            job_list = [{}] * num_active_policies
            job_id_list = []
            for iP in range(num_active_policies):
                # parse policy definitions
                current_policy_solver = active_policies_solvers.iloc[iP]
                inventory_policy_name = current_policy_solver['InventoryPolicyName']
                partition_fields = current_policy_solver['PartitionFields']
                optimization_definition = current_policy_solver['OptimizationDefinition']
                solver_name = current_policy_solver['SolverName']
                solver_path = current_policy_solver['SolverPath']
                file_extension = current_policy_solver['DefinitionFileExtension']
                directory_name = current_policy_solver['DirectoryName']
                log_adl_path = log_adl_folder + '/'+ inventory_policy_name +'/' + current_datetime_string + '/'
                #find all partitions for the current inventory policy and create partition strings
                dirname =  input_adl_folder + '/' + directory_name + '/'
                dir_all = adls_file_system_client.ls(dirname)

                if  isinstance(partition_fields,str) and partition_fields != '':
                    fields = partition_fields.split(',')
                    n_levels = len(fields)
                    #find all partitions
                    for i in range(n_levels-1):
                        dir_all = [adls_file_system_client.ls(dir) for dir in dir_all]
                        #flatten list of directory lists
                        dir_all = [dir for sublist in dir_all for dir in sublist]
                    #create partition strings
                    partitions = [dir.split('/')[-(n_levels):] for dir in dir_all]
                    partitions = [",".join(partitions) for partitions in partitions]
                else:
                    partitions = ['none']

                #create task configuraiton dictionaries for the current inventory policy
                task_num = len(partitions)
                task_list = [{}] * task_num

                for idx, partition in enumerate(partitions):
                    if partition == 'none':
                        taskid = 'task1'
                        log_prefix= 'opt_'
                    else:
                        taskid = 'task_'+'_'.join(partition.split(','))
                        log_prefix = 'opt_' + '_'.join(partition.split(',')) + '_'

                    command_download_scripts  = ['bash -c "python3 /taskscripts/download_scripts.py '
                               '--scripts_adl_dir {} --policy_adl_subdir {} --policy_script {} '
                               '--adl_name {} --adl_tenant_id {} --adl_client_id {} --adl_client_secret {}'.format(
                                   scripts_adl_folder, directory_name, inventory_policy_name+'.py',
                                   _ADL_NAME, _TENANT_ID, _CLIENT_ID, _CLIENT_SECRET)][0]
                    command_optimization = ['python3 /taskscripts/inventory_optimization_task.py '
                               '--input_adl_folder {} --output_adl_folder {} '
                               '--inventory_policy_name {} --optimization_definition {} --directory_name {} '
                               '--solver_name {} --solver_path {} --file_extension {} --partition_str {} '
                               '--adl_name {} --adl_tenant_id {} --adl_client_id {} --adl_client_secret {} --timestamp {}'.format(
                                   input_adl_folder, output_adl_folder,
                                   inventory_policy_name, optimization_definition,directory_name, 
                                   solver_name, solver_path, file_extension, partition,
                                   _ADL_NAME, _TENANT_ID, _CLIENT_ID, _CLIENT_SECRET,current_datetime_string)][0]
                    command_upload_log = ['python3 /taskscripts/upload_to_adls.py '
                               '--local_path {} --remote_path {} --file_names {} --remote_prefix {} '
                               '--adl_name {} --adl_tenant_id {} --adl_client_id {} --adl_client_secret {}"'.format(
                                   '../', log_adl_path,'stdout.txt,stderr.txt', log_prefix,
                                   _ADL_NAME, _TENANT_ID, _CLIENT_ID, _CLIENT_SECRET)][0]
                    command = command_download_scripts + ';'+ command_optimization + ';' + command_upload_log

                    task_list[idx] = {
                            "id": taskid,
                            "image": _CONTAINER_NAME,
                            "remove_container_after_exit": True,
                            "command": command
                            }


                #create job configuraiton dictionary for the current inventory policy
                job_id = _JOB_ID + '_' + inventory_policy_name + '_' + current_datetime_string
                job_id_list.append(job_id)
                job_list[iP] = {
                        "id": job_id,
                        "tasks": task_list
                    }
    
            #Configuration dictionary for all jobs
            jobs = {
                "job_specifications": job_list
            }

            #Write configuration dictionaries to json files
            create_configuration_files(optimization_config_path)
            utils.write_json_to_file(jobs, os.path.join(optimization_config_path, 'jobs.json'))

            #Create Azure Batch Pool using Azure Batch Shipyard
            create_pool_command = 'D:\home\Python35\python.exe batch-shipyard\shipyard.py pool add --yes --configdir '+ optimization_config_path
            subprocess.check_output(create_pool_command)

            #Add optimization jobs to the batch pool
            optimization_shipyard_command = 'D:\home\Python35\python.exe batch-shipyard\shipyard.py jobs add --configdir '+ optimization_config_path
            subprocess.check_output(optimization_shipyard_command)
    
            #Monitor job status using Azure Batch Python SDK
            credentials = ServicePrincipalCredentials(client_id=_CLIENT_ID,
                                                      secret=_CLIENT_SECRET,
                                                      tenant=_TENANT_ID,
                                                      resource="https://batch.core.windows.net/"
                                                      )
            batch_client = batch.BatchServiceClient(
                credentials,
                base_url=_BATCH_ACCOUNT_URL)

            #Wait for job completions
            incomplete_jobs = utils.wait_for_tasks_to_complete(batch_client,
                                       job_id_list,
                                       datetime.timedelta(minutes=optimization_timeout_max))
            
            if not incomplete_jobs:
                print('All optimization jobs completed. Check '+ log_adl_folder +' in ADLS for job logs.')
            else:
                print('The following batch jobs did not complete within '+ str(optimization_timeout_max) + ' minutes and will be deleted.')
                for job_id in incomplete_jobs:
                    print(job_id)
    
            #Delete optimization Azure Batch jobs
            print('Deleting batch jobs...')
            optimization_delete_command = 'D:\home\Python35\python.exe batch-shipyard\shipyard.py jobs del --yes --configdir '+ optimization_config_path
            subprocess.check_output(optimization_delete_command)
    
            time.sleep(60)

            #Delete Azure Batch pool
            print('Deleting batch pool...')
            pool_delete_command =  'D:\home\Python35\python.exe batch-shipyard\shipyard.py pool del --yes --configdir '+ optimization_config_path
            subprocess.check_output(pool_delete_command)
    else:
        print("No inventory policy is scheduled to run in the current period")