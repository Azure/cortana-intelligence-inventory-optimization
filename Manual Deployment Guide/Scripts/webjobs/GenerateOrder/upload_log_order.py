#This script uploads the log of the GenerateOrder Web Job to the webjob_log folder in Azure Data Lake Store
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
import subprocess, os,json

_ADL_NAME = os.environ['DATALAKESTORE_NAME']
_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']

#scripts_local_path = os.path.realpath('.\\taskscripts')
log_adl_folder = '/webjob_log/webjob_generate_order/'
log_file_name = 'output_log.txt'
log_local_path = '%WEBJOBS_DATA_PATH%\%WEBJOBS_RUN_ID%\\'

f = open('datetimestring.txt','r')
current_datetime_string = f.readlines()[0]
f.close()

command_upload_log = ['D:\home\Python35\python.exe upload_to_adls.py '
                       '--local_path {} --remote_path {} --file_names {} --remote_prefix {} '
                       '--adl_name {} --adl_tenant_id {} --adl_client_id {} --adl_client_secret {}'.format(
                            log_local_path, log_adl_folder,log_file_name, current_datetime_string,
                           _ADL_NAME, _TENANT_ID, _CLIENT_ID, _CLIENT_SECRET)][0]

subprocess.check_output(command_upload_log,shell=True)