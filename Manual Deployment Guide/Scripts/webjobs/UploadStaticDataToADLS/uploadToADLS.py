
#Data Uploader for Inventory Optimization Solution How-to

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

# Follow instructions at URL below to create an Active Directory application, grant it ADLS permissions,
# and obtain a client ID and secret:
# https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-authenticate-using-active-directory#create-an-active-directory-application

import sys, os

from azure.datalake.store import core, lib, multithread
import zipfile

cwd = os.getcwd()

#Unzipping the RawData before uploading to Azure DataLake Store
path_to_zip_file =cwd+'\\staticData.zip'
zip_ref = zipfile.ZipFile(path_to_zip_file, 'r')
zip_ref.extractall()
zip_ref.close()


#Azure DataLake Store Credentials
_ADL_NAME = os.environ['DATALAKESTORE_NAME']
_TENANT_ID = os.environ['TENANT_ID']
_CLIENT_ID = os.environ['CLIENT_ID']
_CLIENT_SECRET = os.environ['CLIENT_SECRET']


#localPath='D:\\home\\site\\wwwroot\\app_data\\jobs\\triggered\\uploadStaticData\\staticData\\'

dir_list = next(os.walk('.'+'\\staticData\\'))[1]
token = lib.auth(tenant_id=_TENANT_ID, client_id=_CLIENT_ID, client_secret=_CLIENT_SECRET)
adls_file_system_client = core.AzureDLFileSystem(token, store_name=_ADL_NAME)

for dir in dir_list:
    local_path=cwd+'\\staticData\\'+dir
    print(local_path)
    remote_path=dir+'\\'
    multithread.ADLUploader(adls_file_system_client, lpath=local_path , rpath= remote_path,overwrite=True)


#Update the Schedule file to Main webjob, so that it starts after all the static data is uploaded
fileName='D:\\home\\site\\wwwroot\\app_data\\jobs\\triggered\\Main\\settings.job'
cronJob = '{"schedule":"0 0 0/6 * * *"}'
with open(fileName, "w") as f:
 f.write(cronJob)