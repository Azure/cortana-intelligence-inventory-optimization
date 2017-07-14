#This script invokes the ohter web jobs in a simulation mode. Every time it runs, it passes a new date to the other web jobs.
#For example, if this script is scheduled to run once every hour, the data and results of one day will be generated every hour. 
#This script invokes the other web jobs in the order of Simulator, Evaluation, InventoryOptimization, and Generate Order.

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

import datetime, time, requests,json,os
from azure.datalake.store import core, lib, multithread

#ADL credentials
_ADL_NAME = os.environ['DATALAKESTORE_NAME']
_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']

#Web App credentials
_WEB_APP_NAME = os.environ['FUNCTIONS_APP_NAME']
_WEB_APP_USER = os.environ['FUNCTIONS_APP_USER']
_WEB_APP_PASSWORD = os.environ['FUNCTIONS_APP_PASSWORD']

#Pull the last simulation datetime from ADLS and decide the current simulation datetime
token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
adl = core.AzureDLFileSystem(token=token, store_name=_ADL_NAME)

multithread.ADLDownloader(adl, lpath='LastSimulationDatetime.txt', rpath='/webjob_log/LastSimulationDatetime.txt', overwrite=True)
f = open('LastSimulationDatetime.txt','r')
simulation_datetime_last_str = f.readlines()[0]
f.close()

print('Last simulation time:' + simulation_datetime_last_str)

simulation_datetime_last = datetime.datetime.strptime(simulation_datetime_last_str,'%m/%d/%Y %H:%M:%S')
simulation_datetime_cur = simulation_datetime_last + datetime.timedelta(days=1)
simulation_datetime_cur_str =  datetime.datetime.strftime(simulation_datetime_cur,'%m/%d/%Y %H:%M:%S')

print('Current simulation time:' + simulation_datetime_cur_str)

f = open('LastSimulationDatetime.txt','w')
f.writelines(simulation_datetime_cur_str)
f.close()

multithread.ADLUploader(adl, lpath='LastSimulationDatetime.txt', rpath='/webjob_log/LastSimulationDatetime.txt',overwrite=True)

webjob_simulator = 'Simulator'
webjob_optimization = 'InventoryOptimization'
webjob_order = 'GenerateOrder'
webjob_evaluation = 'Evaluation'

#This function constructs url for calling or get the status of a web job
def construct_url(webjob_name, action):
    if action=='run':
        url = "https://" + _WEB_APP_USER + ":" + _WEB_APP_PASSWORD + "@" + _WEB_APP_NAME + ".scm.azurewebsites.net/api/triggeredwebjobs/" + webjob_name + "/"+ action + "?arguments=\"" + simulation_datetime_cur_str + "\""
    else:
        url = "https://" + _WEB_APP_USER + ":" + _WEB_APP_PASSWORD + "@" + _WEB_APP_NAME + ".scm.azurewebsites.net/api/triggeredwebjobs/" + webjob_name

    return url

#This function monitors the status of a web job and returns the finishing status when it's finished. 
def monitor_webjob(job_name, url):  
    response = requests.get(url)
    job_info = response.json()
    
    print(response)

    if not job_info:
        job_status = 'NA'
    else:
        job_status = job_info["latest_run"]["status"]

    while job_status in ('Running','Initializing','NA'):
        print('Webjob ' + job_name + ' is running. Waiting for 10 seconds.')
        time.sleep (10)
        response = requests.get(url)
        
        print(response)
        
        job_info = response.json()

        if not job_info:
            job_status = 'NA'
        else:
            job_status = job_info["latest_run"]["status"]
            
    if job_status =='Success':
        print('Webjob ' + job_name + ' finished successfully!')
    else:
        print('Webjob ' + job_name + ' finished with status '+job_status+'.')

#This function starts a new web job and monitors it until it's finished.
def webjob_main(webjob_name):
    #start webjob
    run_url = construct_url(webjob_name, 'run')
    response = requests.post(run_url)
    print('Webjob ' + webjob_name + ' started.')
    
    #monitor webjob
    get_url = construct_url(webjob_name, 'get')
    
    #pause 10 seconds before querying job status
    time.sleep (10)
    
    monitor_webjob(webjob_name, get_url)


if __name__ == '__main__':

    start_time_all = time.time()

    #run and monitor simulator webjob
    webjob_main(webjob_simulator)

    #run and monitor optimization webjob
    webjob_main(webjob_optimization)

    #run and monitor order webjob
    webjob_main(webjob_order)

    #run and monitor evaluation webjob
    webjob_main(webjob_evaluation)

    #Compute and save total computation time
    computation_time = time.time() - start_time_all

    computation_time_str = simulation_datetime_cur_str + ',' + str(round(computation_time)) + '\n'

    token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
    adl = core.AzureDLFileSystem(token=token, store_name=_ADL_NAME)
    with adl.open('/webjob_log/ComputationTime.csv','ab') as f:
        f.write(computation_time_str.encode('utf-8'))



