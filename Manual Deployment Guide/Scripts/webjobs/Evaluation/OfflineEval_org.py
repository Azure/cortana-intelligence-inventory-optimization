# -*- coding: utf-8 -*-
"""
Offline Evaluation for Retail Demand Forecasting and Price Optimization Solution How-to
Created on Wed Mar  8 16:26:00 2017

@author: fboylu
"""
import sys,os
import datetime as dt
from datetime import datetime
import pandas as pd
from azure.datalake.store import core, lib
import getopt

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

#sys.path.append("D:\\home\\site\\wwwroot\\site-packages")

# temporary development environment
adl_name = os.environ['DATALAKESTORE_NAME']
tenant_id = os.environ['TENANT_ID']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']

raw_data_folder_sales = 'rawdata_demo'
raw_data_folder_orders = 'orders_demo'

n_stores = 6

def read_file(file_name, timecolumns, adl):
    with adl.open(file_name, 'rb') as f:
        file = pd.read_csv(f) 
    file[timecolumns] = file[timecolumns].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S')
    return file
    
def write_file(file, file_name, adl):
    with adl.open(file_name, 'wb') as f:
        f.write(file.to_csv(index=False, date_format='%Y-%m-%d %H:%M:%S').encode('utf-8'))
        
def read_partial_orders(adl, store_id):
    cols = ['PolicyID', 'StoreID', 'ProductID', 'SupplierID', 'Quantity','OrderTimestamp', 'ETA', 'ConfidenceInterval', 'Fulfilled']
    partial_orders_master = pd.DataFrame(columns=cols)
    policy_folders = adl.ls(raw_data_folder_orders)
     
    for policy in policy_folders:
        file_name = '{}/partial_orders_{}.csv'.format(policy, store_id)
        if adl.exists(file_name):
            with adl.open(file_name, blocksize=2 ** 20) as f:
                policy_orders = pd.read_csv(StringIO(f.read().decode('utf-8')), sep=",", 
                                            parse_dates=['ETA'], 
                                            dtype={'PolicyID':str, 'StoreID':str, 'ProductID':str, 
                                                   'SupplierID':str, 'Quantity':int})
                policy_orders.columns = cols
                partial_orders_master = pd.concat([partial_orders_master, policy_orders])
                partial_orders_master = partial_orders_master.reset_index().iloc[:,1:]
    partial_orders_master['OrderTimestamp'] = pd.to_datetime(partial_orders_master['OrderTimestamp'], format='%Y-%m-%d %H:%M:%S')
    return partial_orders_master

def compute_metric(sales_temp, orders_temp):
    """  
    Go through sales_temp table row by row and find first matching order, if found
    reduce order quantity by one and write order timestamp to sales_temp. Break,
    as no need to look further. Final sales_temp will have the matching order dates
    per row. Return metric per policy and orders_temp table that will be used for 
    next days run as partial orders.    
    """    

    #sales_temp = sales_f    
    #orders_temp = orders_to_date
    
    # sort the two tables by date ascending  
    orders_temp = orders_temp.sort_values(['PolicyID', 'ETA'])
    sales_temp = sales_temp.sort_values('TransactionDateTime')
    sales_orders = pd.DataFrame(columns = sales_temp.columns.tolist() + ['OrderETA','PolicyID'])
    policies = orders_temp.PolicyID.unique() 
    
    for index_s, row_s in sales_temp.iterrows():
        #found = False
        if row_s['Price'] != 0: #not spoilage
            for policy_id in policies:
                orders_policy = orders_temp[orders_temp['PolicyID'] == policy_id]
                for index_o, row_o in orders_policy.iterrows():
                    if ((row_s['ProductID'] == row_o['ProductID']) & (row_o['Quantity'] > 0) & (row_s['TransactionDateTime'] > row_o['ETA'])):
                        orders_temp.ix[index_o,'Quantity'] = row_o['Quantity'] - 1
                        #print(row_o['ProductID'], row_o['Quantity'])
                        temp_row = sales_temp.loc[index_s].copy()
                        temp_row['OrderETA'] = row_o['ETA']
                        temp_row['OrderETA'] = pd.to_datetime(temp_row['OrderETA'], format='%Y-%m-%d %H:%M:%S') #this is to fix strange behaviour with timestamp
                        temp_row['PolicyID'] = row_o['PolicyID']
                        sales_orders = sales_orders.append(temp_row)
                        #found = True
                        break
                #if not found:
                    #print(row_s['ProductID']+ ' is not in inventory')
        else: #spoilage
            for policy_id in policies:
                orders_policy = orders_temp[orders_temp['PolicyID'] == policy_id]
                for index_o, row_o in orders_policy.iterrows():
                    if ((row_s['ProductID'] == row_o['ProductID']) & (row_o['Quantity'] > 0) & (row_s['TransactionDateTime'] > row_o['ETA'])):
                        orders_temp.ix[index_o,'Quantity'] = row_o['Quantity'] - row_s['Units']
                        temp_row = sales_temp.loc[index_s].copy()
                        temp_row['OrderETA'] = row_o['ETA']
                        temp_row['OrderETA'] = pd.to_datetime(temp_row['OrderETA'], format='%Y-%m-%d %H:%M:%S')
                        temp_row['PolicyID'] = row_o['PolicyID']
                        sales_orders = sales_orders.append(temp_row)
                        #found = True
                        break
                #if not found:
                    #print('Spoilage' + row_s['ProductID']+ ' is not in inventory')
    sales_orders = sales_orders.reset_index().iloc[:,1:]            
    # create column for number of days to sale as day fraction
    #sales_orders['DaysToSale'] = sales_orders['TransactionDateTime'] - sales_orders['OrderETA'].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S')
    #sales_orders['DaysToSale'] = sales_orders['DaysToSale'].apply(lambda x: max(x,1))
    #sales_orders['DaysToSale'] = sales_orders['DaysToSale'].dt.total_seconds()/(24*60*60)    
    sales_orders['DaysToSale'] = sales_orders['TransactionDateTime'] - sales_orders['OrderETA'].apply(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S').replace(minute=0, hour=0, second=0))
    sales_orders['DaysToSale'] = sales_orders['DaysToSale'].dt.total_seconds()/(24*60*60)
    sales_orders['DaysToSale'] = sales_orders['DaysToSale'].apply(lambda x: max(x,1)) 
    
    # for spoilages multiply spoilage quantity with DaysToSale 
    sales_orders.loc[sales_orders['Price'] == 0,'DaysToSale'] = sales_orders[sales_orders['Price'] == 0]['DaysToSale'] * sales_orders[sales_orders['Price'] == 0]['Units']
    
    # calculate metric per policy 
    metric_date_time = datetime(sales_orders['TransactionDateTime'][0].year,sales_orders['TransactionDateTime'][0].month, sales_orders['TransactionDateTime'][0].day)
    rows = []
    for policy_id in policies:
        sales_orders_policy = sales_orders[sales_orders['PolicyID'] == policy_id]
        if sales_orders_policy['DaysToSale'].sum() > 0:
            #metric_policy = (sales_orders_policy['Price'].sum()/sales_orders_policy['DaysToSale'].sum())
            sales_orders_policy['NormRevenue'] = sales_orders_policy['Price'] / sales_orders_policy['DaysToSale']
            metric_policy = sales_orders_policy['NormRevenue'].sum() 
        else:
            metric_policy = 0
        metrics_dict ={}
        metrics_dict['PolicyID'] = policy_id
        metrics_dict['MetricDateTime'] = metric_date_time
        metrics_dict['Metric'] = metric_policy                
        rows.append(metrics_dict)      
    
    metrics_df = pd.DataFrame(rows, columns = ['PolicyID','MetricDateTime','Metric'])   
    orders_temp.sort_index(inplace=True) # keep the index of the order table same 
    return metrics_df, orders_temp       # , sales_temp (used for testing purposes)  

def read_compute_write(adl, store_id, file_date_format, offset=False):
    # offset is needed for when the historical orders are already processed
    if offset:
        offset_time = dt.timedelta(days=1)
    else:
        offset_time = dt.timedelta(days=0)
        
    # read sales for the day
    sales_file_name = '{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, file_date_format)
    sales_f = read_file(sales_file_name, 'TransactionDateTime', adl)
                
    # read partial orders 
    orders = read_partial_orders(adl, store_id)   
    # remove when in real time mode and the orders files only have orders up to today
    #orders_to_date = orders[orders['OrderTimestamp'] < end_date + offset_time]
    #orders_after_date = orders[orders['OrderTimestamp'] >= end_date + offset_time]            
            
    #compute metric
    metrics_df, partial_orders_master = compute_metric(sales_f, orders) # change orders_to_date to orders when in real time mode
    #partial_orders_master = partial_orders_master.append(orders_after_date) # remove when in real time mode
        
    # write partial orders for next day with unsold quantities for each order in history
    policies = partial_orders_master['PolicyID'].unique()
    for policy_id in policies:  
        partial_orders_file_name = '{}/{}/partial_orders_{}.csv'.format(raw_data_folder_orders, policy_id, store_id)
        partial_orders_policy = partial_orders_master.loc[partial_orders_master['PolicyID'] == policy_id].copy()
        partial_orders_policy['Quantity'] = partial_orders_policy['Quantity'].astype(int)
        partial_orders_policy['ConfidenceInterval'] = partial_orders_policy['ConfidenceInterval'].astype(int)
        partial_orders_policy.sort_index(inplace=True)
        write_file(partial_orders_policy, partial_orders_file_name, adl)
    
    return metrics_df
    
def calculate_start_end(store_id, today_date):
    # pull start date from orders file 
    #orders_start = read_file('{}/partial_orders_store{}.csv'.format(raw_data_folder_orders,store_id), ['Order timestamp', 'ETA'], adl) 
    orders_start = read_partial_orders(adl, store_id)
    start_date = orders_start.ETA.min() + dt.timedelta(days=1) #sales files are labeled +1 day    
    start_date = datetime(start_date.year, start_date.month, start_date.day)              
    # pull end date from today's sales file 
    sales_end = read_file('{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, today_date.strftime('%Y_%m_%d_%H_%M_%S')), 'TransactionDateTime', adl)        
    end_date = sales_end.TransactionDateTime.min() + dt.timedelta(days=1) #sales files are labeled +1 day       
    end_date = datetime(end_date.year, end_date.month, end_date.day)
    return start_date, end_date
      
def clean_adl():
    # for testing: one time copy of day0 orders file from adl, write back with partial_orders file name
    adl.rm('{}/metric.csv'.format(raw_data_folder_orders))   
    policy_folders = adl.ls(raw_data_folder_orders)     
    for policy in policy_folders:
        for store_id in range(1, n_stores + 1):
            #store_id=1
            partial_orders_file_name = '{}/partial_orders_{}.csv'.format(policy, store_id)
            orders = read_file('{}/orders_{}.csv'.format(policy, store_id), ['Order timestamp', 'ETA'], adl)    
            write_file(orders, partial_orders_file_name, adl)
    

if __name__ == '__main__':   
    try:
        token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        adl = core.AzureDLFileSystem(token=token, store_name=adl_name)
    except Exception as e:
        raise Exception('Error while attempting to connect to Azure Data Lake Store:\n{}'.format(e))  
    #clean_adl()
    
    opts,args = getopt.getopt(sys.argv[1:],"d:",["datetime="])
    for opt, arg in opts:
        if opt in ("-d","--datetime"):
            print(arg)
            # running in BATCH mode
            today_date = datetime.strptime(arg,"%m/%d/%Y %H:%M:%S")
    
    if not ('today_date' in locals() or 'today_date' in globals()):
        # running in PROD mode
        today_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day)

    # for real time : today_date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month, datetime.datetime.today().day, 0, 0)
    #today_date = datetime.datetime(2017, 1, 5, 0, 0) # processing happenes in the evening
    metric_file_name = '{}/metric.csv'.format(raw_data_folder_orders)
     
    # Check if metric file already exists     
    if adl.exists(metric_file_name):  
        for store_id in range(1, n_stores + 1):  
            # store_id = 1
            sales_check = read_file('{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, today_date.strftime('%Y_%m_%d_%H_%M_%S')), 'TransactionDateTime', adl)        
            if not sales_check.empty:
                start_date = today_date - dt.timedelta(days=1)
                end_date = start_date
                #start_date, end_date = calculate_start_end(store_id, today_date)
                file_date_format = today_date.strftime('%Y_%m_%d_%H_%M_%S')  # +1 day for today's sales file name
            
                metrics_df = read_compute_write(adl, store_id, file_date_format, offset=True)          
                metrics_df['StoreID'] = store_id 
                    
                # write metrics_df to metric file
                with adl.open(metric_file_name, 'ab') as f:
                    f.write(metrics_df.to_csv(index=False, header=False).encode('utf-8'))
    else:
        #create empty metric file and write to adl
        metric_file = pd.DataFrame(columns = ['PolicyID','MetricDateTime','Metric', 'StoreID']) 
        write_file(metric_file, metric_file_name, adl)
        
        for store_id in range(1, n_stores + 1): 
            #store_id = 2
            sales_check = read_file('{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, today_date.strftime('%Y_%m_%d_%H_%M_%S')), 'TransactionDateTime', adl)        
            if not sales_check.empty:                       
                start_date = today_date - dt.timedelta(days=1)
                end_date = start_date
                # start_date, end_date = calculate_start_end(store_id, today_date)
                file_date_format = today_date.strftime('%Y_%m_%d_%H_%M_%S') 

                # calculate number of days in history            
                num_days = (end_date - start_date).days + 1
                
                #for file_date_format in [(start_date + datetime.timedelta(days=day)).strftime('%Y_%m_%d_%H_%M_%S') for day in range(0,num_days)]:    
                for x in [1]:    
                    metrics_df = read_compute_write(adl, store_id, file_date_format)
                    metrics_df['StoreID'] = store_id 
                        
                    # write metrics_df to metric file
                    with adl.open(metric_file_name, 'ab') as f:
                        f.write(metrics_df.to_csv(index=False, header=False).encode('utf-8'))