#This script uploads files from specified local directory to specified remote directory on Azure Data Lake Store

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

import os, argparse
from azure.datalake.store import core, lib, multithread

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--local_path', required=True)
    parser.add_argument('--remote_path', required=True)
    parser.add_argument('--file_names', required=True)
    parser.add_argument('--remote_prefix', required=False)
    parser.add_argument('--adl_name', required=True)
    parser.add_argument('--adl_tenant_id', required=True)
    parser.add_argument('--adl_client_id', required=True)
    parser.add_argument('--adl_client_secret', required=True)
    
    args = parser.parse_args()

    local_path =  args.local_path
    remote_path = args.remote_path
    file_names = args.file_names
    adl_name = args.adl_name
    adl_tenant_id = args.adl_tenant_id
    adl_client_id = args.adl_client_id
    adl_client_secret = args.adl_client_secret

    token = lib.auth(tenant_id=adl_tenant_id, client_id=adl_client_id, client_secret=adl_client_secret)
    adls_file_system_client = core.AzureDLFileSystem(token, store_name=adl_name)
    
    file_list = file_names.split(',')
    #upload configuration file, the configuration file was uploaded to root directory, but not visible in the portal
    for file in file_list:
        if args.remote_prefix is not None:
            remote_file = remote_path + args.remote_prefix + file
        else:
            remote_file = remote_path + file
        local_file = os.path.join(local_path,file)
        print('Uploading log file {} to ADL folder [{}]...'.format(local_file, remote_path))    
        multithread.ADLUploader(adls_file_system_client, lpath=local_file , rpath= remote_file,overwrite=True)
