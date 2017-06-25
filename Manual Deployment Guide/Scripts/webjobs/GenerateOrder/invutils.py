#This module contains some helper functions for the inventory optimization solution
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

from azure.datalake.store import core, lib, multithread
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batchauth
import azure.batch.models as batchmodels

from azure.mgmt.datalake.analytics.job import DataLakeAnalyticsJobManagementClient
from azure.mgmt.datalake.analytics.job.models import JobInformation, JobState, USqlJobProperties


#This function saves dictionary to json files
def write_json_to_file(json_dict, filename):
    with open(filename, 'w') as outfile:
        json.dump(json_dict, outfile,indent=4)

#This function checks if an inventory optimization policy needs to be run in the current period
def check_job_trigger(policy_schedule, current_datetime_tuple,current_date):

    #StartDate = datetime.datetime.strptime(policy_schedule['StartDate'],'%m/%d/%Y').date()
    #EndDate = datetime.datetime.strptime(policy_schedule['EndDate'],'%m/%d/%Y').date()
    StartDate = policy_schedule['StartDate'].date()
    EndDate = policy_schedule['EndDate'].date()

    if current_date >= StartDate and current_date<= EndDate:
        cron_cur = policy_schedule['CronExpression']
        job = cronex.CronExpression(cron_cur)
        if job.check_trigger(current_datetime_tuple) or cron_cur=='':
            return 1
        else:
            return 0
    else:
        return 0


#This function monitors the status of all the tasks of the jobs in the job_id_list. 
#It returns a list of incomplete job names if they are not finished within the timeout limit. 
def wait_for_tasks_to_complete(batch_service_client, job_id_list, timeout):
    """
    Returns when all tasks in the specified job reach the Completed state.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id_list: The id of the job whose tasks should be to monitored.
    :param timedelta timeout: The duration to wait for task completion. If all
    tasks in the specified job do not reach Completed state within this time
    period, an exception will be raised.
    """
    timeout_expiration = datetime.datetime.now() + timeout

    print("Monitoring all tasks for 'Completed' state, timeout in {}..."
          .format(timeout), end='')
    
    incomplete_jobs = job_id_list
    while datetime.datetime.now() < timeout_expiration:
        print('.', end='')
        sys.stdout.flush()

        for job_id in incomplete_jobs:
            tasks = batch_service_client.task.list(job_id)
            incomplete_tasks = [task for task in tasks if
                                task.state != batchmodels.TaskState.completed]
            if not incomplete_tasks:
                incomplete_jobs.remove(job_id)
                print('Job '+ job_id + ' completed!')
                
        if not incomplete_jobs: 
            return incomplete_jobs
        else:
            time.sleep(3)

    print()
    return incomplete_jobs

#This function adds a new USQL job to Azure Data Lake analytics
def add_usql_job(scripts_folder, directory_name,usql_file,adl_token,adl_name,simulation_datetime,au_per_usql_job):

    #may need to recreate adl_token every time in case it expires
    #adl_token = lib.auth(tenant_id=adl_tenant_id, client_id=adl_client_id, client_secret=adl_client_secret)

    adla_job_client = DataLakeAnalyticsJobManagementClient(adl_token,  'azuredatalakeanalytics.net')   

    # download USQL file from ADLS
    usql_file_full_path = scripts_folder + '/' + directory_name + '/' + usql_file + '.usql'
    adls_file_system_client = core.AzureDLFileSystem(adl_token, store_name=adl_name)
    multithread.ADLDownloader(adls_file_system_client, lpath = '.', rpath = usql_file_full_path, overwrite=True)
    
    usql_script = ''.join(open(usql_file + '.usql','r').readlines())
    
    if simulation_datetime:
        datetime_replace = "Convert.ToDateTime(\""+ simulation_datetime +"\")"
        usql_script = usql_script.replace('DateTime.Now',datetime_replace)

    jobId = str(uuid.uuid4())
    jobInfo = JobInformation(name = directory_name + '/' + usql_file, type = 'USql',
						             degree_of_parallelism = au_per_usql_job, 
						             properties = USqlJobProperties(script = usql_script))
    jobResult = adla_job_client.job.create(adl_name, jobId, jobInfo)

    return(jobId)

#This class contains all the USQL jobs of an inventory optimization policy
class policy_usql_job:
    def __init__(self, policy, directory,pending_scripts):
        self.policy = policy
        self.directory = directory
        self.pending_scripts = pending_scripts
        self.script_cur = pending_scripts[0]
        self.job_id_cur = None
        self.retry_left = 2
        self.job_status = 'Started'
        self.failed_script = None
        
    def update_job(self, jobid, script):
        self.job_id_cur = jobid
        self.script_cur = script

    def update_job_id(self, jobid):
        self.job_id_cur = jobid

    def finish_job(self):
        self.pending_scripts.remove(self.script_cur)
        if self.pending_scripts:
            self.script_cur = self.pending_scripts[0]
        else:
            self.script_cur = None

    def check_job_remain(self):
        if self.pending_scripts:
            return True
        else:
            return False

    def check_retry_left(self):
        return self.retry_left

    def reduce_retry_left(self):
        self.retry_left = self.retry_left-1

    def reset_retry_left(self):
        self.retry_left = 2

    def mark_failed(self):
        self.job_status = 'Failed'
        self.failed_script = self.script_cur
