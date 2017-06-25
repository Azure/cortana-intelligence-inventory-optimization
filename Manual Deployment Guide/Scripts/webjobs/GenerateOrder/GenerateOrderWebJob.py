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
import datetime, time, os, sys,getopt
import pandas as pd
import json,cronex,uuid,subprocess

#from pyomo.environ import *

from azure.datalake.store import core, lib, multithread

from azure.mgmt.datalake.analytics.job import DataLakeAnalyticsJobManagementClient
from azure.mgmt.datalake.analytics.job.models import JobInformation, JobState, USqlJobProperties

import invutils as utils


#ADL credentials
_ADL_NAME = os.environ['DATALAKESTORE_NAME']
_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']


# ADLA configurations 
au_per_usql_job = os.environ['ADLA_AU_PER_USQL_JOB']  # Analytic Units per USQL script execution

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
    else:
        now = datetime.datetime.now()

    current_date = now.date()
    current_datetime_tuple = (now.year,now.month,now.day,now.hour,0)
    current_datetime_string = datetime.datetime.strftime(now,"%Y%m%d%H%M")
    #current_datetime_string = 'test201704211321'
    f = open('datetimestring.txt','w')
    f.writelines(current_datetime_string)
    f.close()

    #ADLS directories and file names
    scripts_adl_folder = '/inventory_scripts'
    configuration_adl_folder = '/configuration'
    configuration_file_name = 'Configurations.xlsx'
    configuration_adl_path = configuration_adl_folder + '/' + configuration_file_name

    # Create the ADLS client
    adl_token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
    adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=_ADL_NAME)

    #Download configuration file and scripts from Azure Data Lake Store to local
    multithread.ADLDownloader(adls_file_system_client, lpath='.', rpath=configuration_adl_path, overwrite=True)

    #Read downloaded configuration file
    configuration_file_path = os.path.realpath(os.path.join('./', configuration_file_name))
    policy_all = pd.read_excel(configuration_file_path, 'InventoryPolicyConfig')
    solvers_all = pd.read_excel(configuration_file_path, 'SolverConfig')
    schedule_all = pd.read_excel(configuration_file_path, 'ScheduleConfig')

    policy_all = policy_all[policy_all['ActiveFlag'] == 1]

    #find policies to run in the current period
    policy_schedules_all = pd.merge(policy_all,schedule_all, left_on = 'ScheduleID_GenerateOrder',right_on = 'ScheduleID')
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
            usql_generate_orders = current_policy_solver['USQLOrder']
        
            usql_order_files = usql_generate_orders.split(',')
            script_cur = usql_order_files[0]
            policy_usql_job_cur = utils.policy_usql_job(inventory_policy_name,directory_name,usql_order_files)
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
            time.sleep(3)
            print('Waiting for 3 seconds')

        if policy_usql_job_list or failed_usql_job_list:
            if policy_usql_job_list:
                print('The following inventory policy job(s) timed out. Cancelling timeout jobs ...')
                for policy_job in policy_usql_job_list:
                    jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                    if jobResult.state != JobState.ended:
                       adla_job_client.job.cancel(_ADL_NAME, policy_job.job_id_cur)
                       jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                
                    print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql current state: '+ jobResult.result.value) 
            if failed_usql_job_list:
                print('The following inventory policy job(s) failed.')
                for policy_job in failed_usql_job_list:
                    jobResult = adla_job_client.job.get(_ADL_NAME, policy_job.job_id_cur)
                    print('Inventory policy '+ policy_job.policy + ': '+ policy_job.script_cur + '.usql current state: '+ jobResult.result.value)
        else:
            print('All inventory policy USQL jobs finished successfully')
    
    else:
        print("No order is scheduled to be generated in the current period")