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


# -*- coding: utf-8 -*-
"""
Offline Evaluation for Retail Demand Forecasting and Price Optimization Solution How-to
Created on Wed Mar  8 16:26:00 2017

@author: fboylu
"""
import sys
import os
import time
#import datetime as dt
from datetime import datetime
import pandas as pd
from azure.datalake.store import core, lib
import getopt
import numpy as np
from numpy import inf

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

# temporary development environment
adl_name = os.environ['DATALAKESTORE_NAME']
tenant_id = os.environ['TENANT_ID']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']

raw_data_folder_sales = 'rawdata' #'rawdata_demo_walmart'
raw_data_folder_orders = 'orders' #'orders_demo_walmart'

n_stores = 6
n_products = 20

def read_file(file_name, timecolumns, adl):
    with adl.open(file_name, 'rb') as f:
        file = pd.read_csv(f, error_bad_lines=False) 
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
    
    # sort the two tables by date ascending  
    orders_temp = orders_temp.sort_values(['PolicyID', 'ETA'])   
    sales_temp = sales_temp.sort_values('TransactionDateTime')    ; 
    sales_orders = pd.DataFrame(columns = sales_temp.columns.tolist() + ['OrderETA','PolicyID'])
    policies = orders_temp.PolicyID.unique() 
    
    for index_s, row_s in sales_temp.iterrows():
        if row_s['Price'] != 0: #not spoilage
            for policy_id in policies:
                orders_policy = orders_temp[orders_temp['PolicyID'] == policy_id]
                for index_o, row_o in orders_policy.iterrows():
                    if ((row_s['ProductID'] == row_o['ProductID']) & (row_o['Quantity'] > 0) & (row_s['TransactionDateTime'] > row_o['ETA'])):
                        if policy_id == 'Sim' and row_s['ProductID'] == '1_1':
                            print('+++++++++++++++++++++')
                            print(orders_temp.ix[index_o,'Quantity'])
                        orders_temp.ix[index_o,'Quantity'] = row_o['Quantity'] - 1
                        temp_row = sales_temp.loc[index_s].copy()
                        temp_row['OrderETA'] = row_o['ETA']
                        temp_row['OrderETA'] = pd.to_datetime(temp_row['OrderETA'], format='%Y-%m-%d %H:%M:%S') #this is to fix strange behaviour with timestamp
                        temp_row['PolicyID'] = row_o['PolicyID']
                        # add a lable for spoilage
                        temp_row['Spoilage'] = False
                        sales_orders = sales_orders.append(temp_row)    
                        break

        else: #spoilage
            for policy_id in policies:
                orders_policy = orders_temp[orders_temp['PolicyID'] == policy_id]
                for index_o, row_o in orders_policy.iterrows():
                    if ((row_s['ProductID'] == row_o['ProductID']) & (row_o['Quantity'] > 0) & (row_s['TransactionDateTime'] > row_o['ETA'])):
                        orders_temp.ix[index_o,'Quantity'] = max(row_o['Quantity'] - row_s['Units'], 0) # may cause negative value
                        #orders_temp.ix[index_o,'Quantity'] = row_o['Quantity'] - row_s['Units']
                        temp_row = sales_temp.loc[index_s].copy()
                        temp_row['OrderETA'] = row_o['ETA']
                        temp_row['OrderETA'] = pd.to_datetime(temp_row['OrderETA'], format='%Y-%m-%d %H:%M:%S')
                        temp_row['PolicyID'] = row_o['PolicyID']
                        # add a lable for spoilage
                        temp_row['Spoilage'] = True
                        sales_orders = sales_orders.append(temp_row)  
                        break
    sales_orders = sales_orders.reset_index().iloc[:,1:]            
    # create column for number of days to sale as day fraction
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
        #print(sales_orders_policy.head(n=5))
        if sales_orders_policy['DaysToSale'].sum() > 0:
            #metric_policy = (sales_orders_policy['Price'].sum()/sales_orders_policy['DaysToSale'].sum())
            sales_orders_policy['NormRevenue'] = sales_orders_policy['Price'] / sales_orders_policy['DaysToSale']
            metric_policy = sales_orders_policy['NormRevenue'].sum()            
            # compute total revenue
            total_revenue_policy = sales_orders_policy['Price'].sum()
            print("---------------------------")
            print(sales_orders_policy[sales_orders_policy['ProductID']=='1_1'].shape[0])
        else:
            metric_policy = 0
            total_revenue_policy = 0
        metrics_dict ={}
        metrics_dict['PolicyID'] = policy_id
        metrics_dict['MetricDateTime'] = metric_date_time
        metrics_dict['Metric'] = metric_policy       
        metrics_dict['TotalRevenue'] = total_revenue_policy   

        rows.append(metrics_dict)      
    
    metrics_df = pd.DataFrame(rows, columns = ['PolicyID','MetricDateTime','Metric','TotalRevenue'])   
    orders_temp.sort_index(inplace=True) # keep the index of the order table same 
    return metrics_df, orders_temp, sales_orders       

def read_compute_write(adl, store_id, file_date_format):    
    # read sales for the day
    sales_file_name = '{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, file_date_format)
    sales_f = read_file(sales_file_name, 'TransactionDateTime', adl)
                
    # read partial orders 
    orders = read_partial_orders(adl, store_id)   
            
    #compute metric
    metrics_df, partial_orders_master, sales_orders = compute_metric(sales_f, orders) # change orders_to_date to orders when in real time mode
    #write_file(sales_orders, '{}/sales_orders{}_{}.csv'.format(raw_data_folder_orders, store_id, file_date_format), adl)     

    # write partial orders for next day with unsold quantities for each order in history
    policies = partial_orders_master['PolicyID'].unique()
    for policy_id in policies:  
        partial_orders_file_name = '{}/{}/partial_orders_{}.csv'.format(raw_data_folder_orders, policy_id, store_id)
        partial_orders_policy = partial_orders_master.loc[partial_orders_master['PolicyID'] == policy_id].copy()
        partial_orders_policy['Quantity'] = partial_orders_policy['Quantity'].astype(int)
        partial_orders_policy['ConfidenceInterval'] = partial_orders_policy['ConfidenceInterval'].astype(int)
        partial_orders_policy.sort_index(inplace=True)
        write_file(partial_orders_policy, partial_orders_file_name, adl)   
    return metrics_df, sales_orders

# get the number of stockout events on a certain day for each store under all policies
# TBD: combine two dates in the inoput into one date 
def get_num_stockout(adl, store_id, file_date_format, file_date_format_forecast, sales_orders):
    # read sales for the day
    #sales_file_name = '{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, file_date_format)
    #sales_temp = read_file(sales_file_name, 'TransactionDateTime', adl)
    sales_temp = sales_orders[sales_orders['Spoilage']==False]
    
    # read partial orders 
    orders_temp = read_partial_orders(adl, store_id)   
    
    # sort the two tables by date ascending  
    orders_temp = orders_temp.sort_values(['PolicyID', 'ETA'])
    sales_temp = sales_temp.sort_values('TransactionDateTime')
    #sales_orders = pd.DataFrame(columns = sales_temp.columns.tolist() + ['OrderETA','PolicyID'])
    policies = orders_temp.PolicyID.unique() 
    
    # calculate number of stockout items per policy 
    rows = []
    rows_inventory = []
    for policy_id in policies:
        metrics_dict ={}
        metrics_dict['NumStockout'] = 0
        print(policy_id)
        #inventory_df = orders_temp.loc[(orders_temp['PolicyID']==policy_id) & (orders_temp['ETA'].apply(lambda x: x.date()) <= pd.to_datetime(file_date_format, format='%Y_%m_%d_%H_%M_%S').date())]
        file_date_format_round = pd.to_datetime(file_date_format, format='%Y_%m_%d_%H_%M_%S') 
        file_date_format_round = datetime(file_date_format_round.year, file_date_format_round.month, file_date_format_round.day, 0, 0, 0)
        inventory_df = orders_temp.loc[(orders_temp['PolicyID']==policy_id) & (orders_temp['ETA'] <= (pd.to_datetime(file_date_format_round, format='%Y_%m_%d_%H_%M_%S') + pd.DateOffset(1)))]
        #print(inventory_df) 
        for product_id in range(1, n_products + 1): 
            inventory_df_product = inventory_df.loc[inventory_df['ProductID']==str(product_id)+'_1']     
            cur_inventory = inventory_df_product['Quantity'].sum()      
            rows_inventory.append({'PolicyID': policy_id, 'DateTime': pd.to_datetime(file_date_format, format='%Y_%m_%d_%H_%M_%S').date(), 'StoreID': store_id, 'ProductID': product_id, 'Inventory': cur_inventory})            
                   
            # check the demand and sales if current inventory is empty                                
            if cur_inventory == 0:
                # read predicted demand
                # use the previous day demand because today's sales file actually stores yesterday's sales
                prev_date = pd.to_datetime(file_date_format_forecast, format='%Y-%m-%d_%H_%M_%S') -  pd.DateOffset(1)
                prev_date = prev_date.strftime('%Y-%m-%d_%H_%M_%S')
                demand_file_name = '{}/demand_forecasts/{}/{}_1/{}.csv'.format(raw_data_folder_sales, store_id, product_id, prev_date)
                demand_df = read_file(demand_file_name, 'DateTime', adl)
                cur_demand = demand_df['Demand'][0] 
                # get current sales
                sales_temp_policy = sales_temp.loc[sales_temp['PolicyID']==policy_id]
                cur_sales = sales_temp_policy.loc[sales_temp_policy['ProductID']==str(product_id)+'_1']['Units'].sum()
                metrics_dict['NumStockout'] += max(int(cur_demand) - cur_sales, 0)
                print(cur_demand)
                print(cur_sales)
                print('*********')
                        
        rows.append(metrics_dict)      
    
    num_stockout_df = pd.DataFrame(rows, columns = ['NumStockout'])   
    orders_temp.sort_index(inplace=True) # keep the index of the order table same 
    inventory_store = pd.DataFrame(rows_inventory, columns = ['PolicyID','DateTime','StoreID','ProductID','Inventory'])
    
    return num_stockout_df, inventory_store 

def metric_today(adl, today_date):
    print("Compute metric of today")
    inventory_file_name = '{}/inventory.csv'.format(raw_data_folder_orders)
    cols = ['PolicyID','DateTime','StoreID','ProductID','Inventory']
    inventory_master = pd.DataFrame(columns=cols)    
    if adl.exists(inventory_file_name):
        with adl.open(inventory_file_name, 'rb') as f:
            inventory_file = pd.read_csv(f, error_bad_lines=False)  
            if inventory_file.shape[0] != 0:
                print("Inventory file exists and contains records.")
                inventory_avg_by_stores = inventory_file.groupby('PolicyID')['Inventory'].mean()
            #print("ooooooooooooooooooooo")
            #print(inventory_avg_by_stores)     
        
    for store_id in range(1, n_stores + 1): 
            sales_check = read_file('{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, today_date.strftime('%Y_%m_%d_%H_%M_%S')), 'TransactionDateTime', adl)      
            print("Check if sales file is empty")
            print('{}/sales_store{}_{}.csv'.format(raw_data_folder_sales, store_id, today_date.strftime('%Y_%m_%d_%H_%M_%S')))
            print(sales_check.empty)
            if not sales_check.empty:                       
                file_date_format = today_date.strftime('%Y_%m_%d_%H_%M_%S') 
                metrics_df, sales_orders = read_compute_write(adl, store_id, file_date_format)
                # get number of stockout events
                file_date_format_forecast = today_date.strftime('%Y-%m-%d_%H_%M_%S') 
                num_stockout_store, inventory_store = get_num_stockout(adl, store_id, file_date_format, file_date_format_forecast, sales_orders)
                inventory_master = pd.concat([inventory_master, inventory_store])
                metrics_df['NumStockout'] = num_stockout_store

                if not adl.exists(inventory_file_name):
                    #inventory_store_select = inventory_store[inventory_store['ProductID'] in ['1_1', '2_1', '3_1', '4_1', '5_1']]  # temporary 
                    inventory_avg_by_stores = inventory_store.groupby('PolicyID')['Inventory'].mean()
                else:
                    with adl.open(inventory_file_name, 'rb') as f:
                        inventory_file = pd.read_csv(f, error_bad_lines=False)  
                    if inventory_file.shape[0] == 0:
                        inventory_avg_by_stores = inventory_store.groupby('PolicyID')['Inventory'].mean()

                metrics_df['TurnoverRatio'] = metrics_df['TotalRevenue'].values / inventory_avg_by_stores.values 
                metrics_df['StoreID'] = store_id                
                # write metrics_df to metric file
                with adl.open(metric_file_name, 'ab') as f:
                    f.write(metrics_df.to_csv(index=False, header=False).encode('utf-8'))
                    
    # write the inventory file
    if not adl.exists(inventory_file_name):
        inventory_file_header = pd.DataFrame(columns = ['PolicyID','DateTime','StoreID','ProductID','Inventory']) 
        write_file(inventory_file_header, inventory_file_name, adl)    

    # write inventory_master to inventory file
    with adl.open(inventory_file_name, 'ab') as f:
        f.write(inventory_master.to_csv(index=False, header=False).encode('utf-8')) 
        
         
def get_metric_change(summary_metric):
    column_names = ['MetricIncrease','TotalRevenueIncrease','NumStockoutDecrease','TurnoverRatioIncrease']
    metric_change = pd.DataFrame(columns=column_names) 
    
    baseline_metric = np.tile(summary_metric[summary_metric['PolicyID']=='Sim']['Metric'].values, len(summary_metric['PolicyID'].unique()))
    optimize_metric = summary_metric['Metric'].values
    relative_change = (optimize_metric - baseline_metric) / baseline_metric
    relative_change[(relative_change == inf) | (relative_change == -inf)] = 1 # default value when baseline metric is 0
    #print(baseline_metric)
    #print(optimize_metric)
    metric_change['MetricIncrease'] = relative_change * 100

    baseline_metric = np.tile(summary_metric[summary_metric['PolicyID']=='Sim']['TotalRevenue'].values, len(summary_metric['PolicyID'].unique()))
    optimize_metric = summary_metric['TotalRevenue'].values
    relative_change = (optimize_metric - baseline_metric) / baseline_metric
    relative_change[(relative_change == inf) | (relative_change == -inf)] = 1 # default value when baseline metric is 0
    metric_change['TotalRevenueIncrease'] = relative_change * 100

    baseline_metric = np.tile(summary_metric[summary_metric['PolicyID']=='Sim']['NumStockout'].values, len(summary_metric['PolicyID'].unique()))
    optimize_metric = summary_metric['NumStockout'].values
    relative_change = - (optimize_metric - baseline_metric) / baseline_metric
    relative_change[(relative_change == inf) | (relative_change == -inf)] = 1 # default value when baseline metric is 0
    metric_change['NumStockoutDecrease'] = relative_change * 100

    baseline_metric = np.tile(summary_metric[summary_metric['PolicyID']=='Sim']['TurnoverRatio'].values, len(summary_metric['PolicyID'].unique()))
    optimize_metric = summary_metric['TurnoverRatio'].values
    relative_change = (optimize_metric - baseline_metric) / baseline_metric
    relative_change[(relative_change == inf) | (relative_change == -inf)] = 1 # default value when baseline metric is 0
    metric_change['TurnoverRatioIncrease'] = relative_change * 100

    return metric_change

def write_summary_metric(adl, today_date):
    days_per_week = 7
    days_per_month = 30 
    days_per_quarter = 91
    metric_file_name = '{}/metric_test.csv'.format(raw_data_folder_orders)
    with adl.open(metric_file_name, 'rb') as f:
        metric_file = pd.read_csv(f, error_bad_lines=False) 
    # Check if the metric file is empty
    if metric_file.shape[0] == 0:
        return#sys.exit(0)
    metric_file['MetricDateTime'] = metric_file['MetricDateTime'].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S')
    column_names = ['MetricDateTime','EvalPeriod','PolicyID','StoreID','Metric','TotalRevenue','NumStockout','TurnoverRatio',
                    'MetricIncrease','TotalRevenueIncrease','NumStockoutDecrease','TurnoverRatioIncrease']
   
    #create new summary metric file and write to adl
    #summary_metric_header = pd.DataFrame(columns = column_names) 
    #write_file(summary_metric_header, summary_metric_file_name, adl) 
    
    # get last week's metric
    summary_metric_week = pd.DataFrame(columns=column_names) 
    last_week_metric = metric_file.loc[(metric_file['MetricDateTime'] <= today_date) & (metric_file['MetricDateTime'] >= today_date - pd.DateOffset(days_per_week))]                                   
    #avg_metric_last_week = metric_file.groupby(['PolicyID','StoreID'])['Metric'].mean().to_frame('Metric').reset_index() 
    avg_metric_last_week = last_week_metric.groupby(['PolicyID','StoreID'])['Metric','TotalRevenue','NumStockout','TurnoverRatio'].mean().reset_index()
    summary_metric_week['PolicyID'] = avg_metric_last_week['PolicyID']
    summary_metric_week['StoreID'] = avg_metric_last_week['StoreID']
    summary_metric_week['Metric'] = avg_metric_last_week['Metric']
    summary_metric_week['TotalRevenue'] = avg_metric_last_week['TotalRevenue']
    summary_metric_week['NumStockout'] = avg_metric_last_week['NumStockout']
    summary_metric_week['TurnoverRatio'] = avg_metric_last_week['TurnoverRatio']
    summary_metric_week['MetricDateTime'] = today_date
    summary_metric_week['EvalPeriod'] = '(I) LastWeek'
    metric_change = get_metric_change(summary_metric_week)
    summary_metric_week['MetricIncrease'], summary_metric_week['TotalRevenueIncrease'], summary_metric_week['NumStockoutDecrease'], summary_metric_week['TurnoverRatioIncrease'] = metric_change['MetricIncrease'], metric_change['TotalRevenueIncrease'], metric_change['NumStockoutDecrease'], metric_change['TurnoverRatioIncrease']
    
    
    # get last month's metric
    summary_metric_month = pd.DataFrame(columns=column_names) 
    last_month_metric = metric_file.loc[(metric_file['MetricDateTime'] <= today_date) & (metric_file['MetricDateTime'] >= today_date - pd.DateOffset(days_per_month))]                                    
    avg_metric_last_month = last_month_metric.groupby(['PolicyID','StoreID'])['Metric','TotalRevenue','NumStockout','TurnoverRatio'].mean().reset_index()
    summary_metric_month['PolicyID'] = avg_metric_last_month['PolicyID']
    summary_metric_month['StoreID'] = avg_metric_last_month['StoreID']
    summary_metric_month['Metric'] = avg_metric_last_month['Metric']
    summary_metric_month['TotalRevenue'] = avg_metric_last_month['TotalRevenue']
    summary_metric_month['NumStockout'] = avg_metric_last_month['NumStockout']
    summary_metric_month['TurnoverRatio'] = avg_metric_last_month['TurnoverRatio']
    summary_metric_month['MetricDateTime'] = today_date
    summary_metric_month['EvalPeriod'] = '(II) LastMonth'   
    metric_change = get_metric_change(summary_metric_month)
    summary_metric_month['MetricIncrease'], summary_metric_month['TotalRevenueIncrease'], summary_metric_month['NumStockoutDecrease'], summary_metric_month['TurnoverRatioIncrease'] = metric_change['MetricIncrease'], metric_change['TotalRevenueIncrease'], metric_change['NumStockoutDecrease'], metric_change['TurnoverRatioIncrease']
    

    # get last quarter's metric
    summary_metric_quarter = pd.DataFrame(columns=column_names) 
    last_quarter_metric = metric_file.loc[(metric_file['MetricDateTime'] <= today_date) & (metric_file['MetricDateTime'] >= today_date - pd.DateOffset(days_per_quarter))]                                    
    avg_metric_last_quarter = last_quarter_metric.groupby(['PolicyID','StoreID'])['Metric','TotalRevenue','NumStockout','TurnoverRatio'].mean().reset_index()
    summary_metric_quarter['PolicyID'] = avg_metric_last_quarter['PolicyID']
    summary_metric_quarter['StoreID'] = avg_metric_last_quarter['StoreID']
    summary_metric_quarter['Metric'] = avg_metric_last_quarter['Metric']
    summary_metric_quarter['TotalRevenue'] = avg_metric_last_quarter['TotalRevenue']
    summary_metric_quarter['NumStockout'] = avg_metric_last_quarter['NumStockout']
    summary_metric_quarter['TurnoverRatio'] = avg_metric_last_quarter['TurnoverRatio']
    summary_metric_quarter['MetricDateTime'] = today_date
    summary_metric_quarter['EvalPeriod'] = '(III) LastQuarter'   
    metric_change = get_metric_change(summary_metric_quarter)
    summary_metric_quarter['MetricIncrease'], summary_metric_quarter['TotalRevenueIncrease'], summary_metric_quarter['NumStockoutDecrease'], summary_metric_quarter['TurnoverRatioIncrease'] = metric_change['MetricIncrease'], metric_change['TotalRevenueIncrease'], metric_change['NumStockoutDecrease'], metric_change['TurnoverRatioIncrease']
                                
    summary_metric_all = pd.concat([summary_metric_week, summary_metric_month, summary_metric_quarter])
    
    
    # write summary_metric_all to summary metric file
    summary_metric_file_name = '{}/summary_metric_test.csv'.format(raw_data_folder_orders) 
    with adl.open(summary_metric_file_name, 'ab') as f:
        f.write(summary_metric_all.to_csv(index=False, header=True).encode('utf-8')) 
                    
def clean_adl():
    # for testing: one time copy of day0 orders file from adl, write back with partial_orders file name
    if adl.exists('{}/metric_test.csv'.format(raw_data_folder_orders)):
        adl.rm('{}/metric_test.csv'.format(raw_data_folder_orders)) 
    if adl.exists('{}/inventory.csv'.format(raw_data_folder_orders)):
        adl.rm('{}/inventory.csv'.format(raw_data_folder_orders))  
    policy_folders = adl.ls(raw_data_folder_orders)     
    def is_folder(item_name):
        return '.' not in item_name 
    
    for policy in policy_folders:
        if is_folder(policy):
            for store_id in range(1, n_stores + 1):
                #store_id=1
                partial_orders_file_name = '{}/partial_orders_{}.csv'.format(policy, store_id)
                orders_file_name = '{}/orders_{}.csv'.format(policy, store_id)
                print(orders_file_name)
                with adl.open(orders_file_name, 'rb') as f:
                    column_names = list(pd.read_csv(f))
                    if 'Order timestamp' not in column_names:
                        selected_columns = ['OrderTimestamp', 'ETA']
                    else:
                        selected_columns = ['Order timestamp', 'ETA']
                f.close()    
                orders = read_file('{}/orders_{}.csv'.format(policy, store_id), selected_columns, adl)    
                write_file(orders, partial_orders_file_name, adl)                    
    
                
if __name__ == '__main__':   
    print("main function started")
    start_time = time.time()
    try:
        token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        adl = core.AzureDLFileSystem(token=token, store_name=adl_name)
    except Exception as e:
        raise Exception('Error while attempting to connect to Azure Data Lake Store:\n{}'.format(e))
    
    # Check if the orders folder exists and the order files are inside    
    if not adl.exists(raw_data_folder_orders):
        sys.exit(0)
    else:    
        policy_folders = adl.ls(raw_data_folder_orders)     
        def is_folder(item_name):
            return '.' not in item_name       
        for policy in policy_folders:
            if is_folder(policy):
                if len(adl.ls(policy)) == 0:
                    sys.exit(0)
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
    print(today_date)
    
    metric_file_name = '{}/metric_test.csv'.format(raw_data_folder_orders)
    # Check if metric file already exists     
    if adl.exists(metric_file_name):  
        metric_today(adl, today_date)
    else:
        #create empty metric file and write to adl
        metric_file = pd.DataFrame(columns = ['PolicyID','MetricDateTime','Metric','TotalRevenue','NumStockout','TurnoverRatio','StoreID']) 
        write_file(metric_file, metric_file_name, adl)      
        metric_today(adl, today_date)

    summary_metric_file_name = '{}/summary_metric_test.csv'.format(raw_data_folder_orders)    
    # Check if summary metric file already exists     
    if adl.exists(summary_metric_file_name):
        adl.rm('{}/summary_metric_test.csv'.format(raw_data_folder_orders))   
    write_summary_metric(adl, today_date)
 
    print("--- %s seconds ---" % (time.time() - start_time))
