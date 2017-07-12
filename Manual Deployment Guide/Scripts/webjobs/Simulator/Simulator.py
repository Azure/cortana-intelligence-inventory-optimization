#Data simulator for Inventory Optimization Solution How-to

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

import sys

import datetime as dt
import os, json, codecs, random
import pandas as pd
from io import StringIO
from copy import deepcopy
from azure.datalake.store import core, lib, multithread
import numpy as np
from numpy.random import uniform, random_integers, choice
from collections import Counter
from datetime import datetime
import getopt

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

random.seed(1)

##############################################
# configuration parameters
##############################################

raw_data_folder = 'rawdata'
public_parameters_folder = 'publicparameters'
private_parameters_folder = 'privateparameters'
orders_folder = 'orders'
configuration_folder = 'configuration'
hierarchy_file = 'hierarchy_invopt.json'

# Dynamic starting date: 14 days ago
n_weeks_to_simulate = 1
n_weeks_to_forecast = 6

opts,args = getopt.getopt(sys.argv[1:],"d:",["datetime="])
for opt, arg in opts:
    if opt in ("-d","--datetime"):
        print(arg)
        # running in BATCH mode
        today_date = datetime.strptime(arg,"%m/%d/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")

if not ('today_date' in locals() or 'today_date' in globals()):
    # running in PROD mode
    today_date = datetime(datetime.today().year, datetime.today().month, datetime.today().day).strftime("%Y-%m-%d %H:%M:%S")


adl_name = os.environ['DATALAKESTORE_NAME']
tenant_id = os.environ['TENANT_ID']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
 
##############################################
#    simulation parameters
##############################################

n_stores = 6
n_brands = 20
n_departments = 4
n_suppliers = 5
products_per_brand = 1
loss_rate = 0

# store department parameters
min_price_elasticity = -1.3  # price elasticity is defined per department
max_price_elasticity = -0.7  # price elasticity is the same for all brands in the same department

# storage parameters
n_storage_spaces = 4  # number of storage spaces in each store
min_storage_volume = 100
max_storage_volume = 200
min_storage_budget = 1000
max_storage_budget = 2000
min_storage_cost = 1
max_storage_cost = 5
min_min_inventory_size = 0
max_min_inventory_size = 100
min_inventory_size_interval = 0
max_inventory_size_interval = 1000
min_missed_sale_cost = 1
max_missed_sale_cost = 10

# supplier parameters
min_shipping_cost = 1
max_shipping_cost = 10
min_min_shipping_volume = 0
max_min_shipping_volume= 100
min_shipping_volume_interval = 0
max_shipping_volume_interval = 1000
min_fixed_order_size = 1
max_fixed_order_size = 1000
min_purchase_cost_budget = 1000
max_purchase_cost_budget = 1000000

# product and brand parameters
min_brand_desirability = 0.7
max_brand_desirability = 1.3
min_product_volume = 0.1
max_product_volume = 2
min_shelf_life = 1
max_shelf_life = 7
min_msrp_multiplier = 1.1
max_msrp_multiplier = 1.5

# product supplier parameters
min_lead_time = 1
max_lead_time = 3
min_lead_time_conf_interval = 0
max_lead_time_conf_interval = 1
min_min_order_quantity = 0
max_min_order_quantity = 10
min_order_quantity_interval = 100
max_order_quantity_interval = 100000
min_quantity_multiplier = 1
max_quantity_multiplier = 20
min_purchase_cost = 2
max_purchase_cost = 20
min_backorder_multiplier = 1
max_backorder_multiplier = 1.5
min_shipping_multiplier = 0.01
max_shipping_multiplier = 0.5
min_purchase_cost_budget_multiplier = 100
max_purchase_cost_budget_multiplier = 10000
min_ordering_frequency = 1
max_ordering_frequency = 10
min_service_level = 0.9
max_service_level = 0.9999
min_disposal_multiplier = 0.1
max_disposal_multiplier = 0.2

# auxiliary function for writing data to csv files
def write_data(list_of_dict, fields, adl, file_name):
    rows = []
    for row in list_of_dict:
        new_row = [row[i][fields[i]] for i in range(len(row))]
        new_row.extend([row[-1][x] for x in fields[len(row):]])
        rows.append(new_row)
    rows_df = pd.DataFrame(rows, columns=fields)
    with adl.open(file_name, 'wb') as f:
        f.write(rows_df.to_csv(index=False).encode('utf-8'))


# define policy_name of inventory management policy that manager product in a store
def get_policy_name(store, product, supplier):
    return "Sim"


# definitions of static data (suppliers, products, brands, stores, storage)
class AttributeDescription:

    # definitions of brands and products
    def __define_brands_products(self):
        
        # definition of brands and products
        self.hierarchy['Brands'] = []
        for BrandID in range(1, n_brands + 1):
            brand_dict = {}
            brand_dict['BrandID'] = str(BrandID)
            brand_dict['BrandName'] = 'Brand ' + brand_dict['BrandID']
            brand_dict['Desirability'] = uniform(min_brand_desirability, max_brand_desirability)

            # definition of products of the given brand in the given store department
            brand_dict['Products'] = []

            # For the time being, only one product per brand. This will change in the future 
            product_dict = {}
            product_dict['ProductID'] =  brand_dict['BrandID'] + '_1'
            product_dict['ProductName'] =  brand_dict['BrandName'] + ' Product ' + product_dict['ProductID']
            product_dict['ProductVolume'] = uniform(min_product_volume, max_product_volume)
            product_dict['MSRP'] = 0 # will be updated later on, based on the purchase cost
            if BrandID <= n_brands/2 or choice([-1,1]) == 1: # first half of the brands have perishable products
                                                             # some brands in the second half also have perishable products
                product_dict['ShelfLife'] = str(random_integers(min_shelf_life, max_shelf_life)) + ' days'
            else:
                product_dict['ShelfLife'] = '10000 days'

            brand_dict['Products'].append(product_dict)

            self.hierarchy['Brands'].append(brand_dict)


    # definitions of suppliers
    def __define_suppliers(self):

        self.hierarchy['Suppliers'] = []
        for SupplierID in range(1, n_suppliers + 1):
            supplier_dict = {}
            supplier_dict['SupplierID'] = str(SupplierID)
            supplier_dict['SupplierName'] = 'Supplier ' + supplier_dict['SupplierID']
            supplier_dict['ShippingCost'] = uniform(min_shipping_cost, max_shipping_cost)
            supplier_dict['MinShippingVolume'] = uniform(min_min_shipping_volume, max_min_shipping_volume)
            supplier_dict['MaxShippingVolume'] = supplier_dict['MinShippingVolume'] + uniform(min_shipping_volume_interval, max_shipping_volume_interval)
            supplier_dict['FixedOrderSize'] = int(random_integers(min_fixed_order_size, max_fixed_order_size))
            supplier_dict['PurchaseCostBudget'] = uniform(min_purchase_cost_budget, max_purchase_cost_budget)

            self.hierarchy['Suppliers'].append(supplier_dict)


    # definitions of storage
    def __store_storage(self):
        storage = []
        for StorageID in range(1, n_storage_spaces + 1):
            storage_dict = {}
            storage_dict['StorageID'] = str(StorageID)
            storage_dict['StorageName'] = 'Storage ' + storage_dict['StorageID']
            storage_dict['StorageVolume'] = uniform(min_storage_volume, max_storage_volume)
            storage_dict['StorageCostBudget'] = uniform(min_storage_budget, max_storage_budget)
            storage.append(storage_dict)

        return storage


    # definitions of departments
    def __store_departments(self):

        departments = []
        for DepartmentID in range(1, n_departments + 1):
            department_dict = {}
            department_dict['DepartmentID'] = str(DepartmentID)
            department_dict['DepartmentName'] = 'Department ' + department_dict['DepartmentID']
            department_dict['PriceElasticity'] = uniform(min_price_elasticity, max_price_elasticity)
 
            # add products to departments. Each product can only be in one department
            start_brand = int((DepartmentID - 1) * n_brands / n_departments)
            end_brand = int(DepartmentID * n_brands / n_departments)
            department_dict['Brands'] = deepcopy(self.hierarchy['Brands'][start_brand : end_brand]) # we use deepcopy to create a copy of a product inside department
                                                                                                    # this will allow us to have different MSRP for the same product in different stores

            departments.append(department_dict)

        return departments


    # definitions of product storage
    def __store_product_storage(self):

        product_storage = []
        for StorageID in range(1, n_storage_spaces + 1):
            start_brand = int((StorageID - 1) * n_brands / n_storage_spaces)
            end_brand = int(StorageID * n_brands / n_storage_spaces)
            storage_dict = {}
            storage_dict['StorageID'] = StorageID
            storage_dict['Products'] = []
  
            # place products in a given storage space
            for BrandID in range(start_brand, end_brand):
                for product in self.hierarchy['Brands'][BrandID]['Products']:
                    product_storage_dict = {}
                    product_storage_dict['ProductID'] = product['ProductID']
                    product_storage_dict['StorageCost'] = uniform(min_storage_cost, max_storage_cost)
                    product_storage_dict['MissedSaleCost'] = uniform(min_missed_sale_cost, max_missed_sale_cost)
                    product_storage_dict['MinInventorySize'] = int(random_integers(min_min_inventory_size, max_min_inventory_size))
                    product_storage_dict['MaxInventorySize'] = product_storage_dict['MinInventorySize'] + int(random_integers(min_inventory_size_interval, max_inventory_size_interval))

                    storage_dict['Products'].append(product_storage_dict)

            product_storage.append(storage_dict)

        return product_storage


    # definitions of suppliers of products
    def __store_product_supplier(self):

        product_supplier = []
        for SupplierID in range(1, n_suppliers + 1): 
            start_brand = int((SupplierID - 1) * n_brands / n_suppliers)
            end_brand = int(SupplierID * n_brands / n_suppliers)
            supplier_dict = {}
            supplier_dict['SupplierID'] = SupplierID
            supplier_dict['Products'] = []

            # place products in a given storage space
            for BrandID in range(start_brand, end_brand):
                for product in self.hierarchy['Brands'][BrandID]['Products']:
                    product_supplier_dict = {}
                    product_supplier_dict['ProductID'] = product['ProductID']
                    product_supplier_dict['LeadTime'] = int(random_integers(min_lead_time, max_lead_time))
                    product_supplier_dict['LeadTimeConfidenceInterval'] = int(random_integers(min_lead_time_conf_interval, max_lead_time_conf_interval))
                    product_supplier_dict['MinOrderQuantity'] = int(random_integers(min_min_order_quantity, max_min_order_quantity))
                    product_supplier_dict['MaxOrderQuantity'] = product_supplier_dict['MinOrderQuantity'] + int(random_integers(min_order_quantity_interval, max_order_quantity_interval))
                    product_supplier_dict['QuantityMultiplier'] = int(random_integers(min_quantity_multiplier, max_quantity_multiplier))
                    product_supplier_dict['Cost'] = uniform(min_purchase_cost, max_purchase_cost)
                    product_supplier_dict['BackorderCost'] = product_supplier_dict['Cost'] * uniform(min_backorder_multiplier, max_backorder_multiplier)
                    product_supplier_dict['PurchaseCostBudget'] = product_supplier_dict['Cost'] * uniform(min_purchase_cost_budget_multiplier, max_purchase_cost_budget_multiplier)
                    product_supplier_dict['ShippingCost'] = product_supplier_dict['Cost'] * uniform(min_shipping_multiplier, max_shipping_multiplier)
                    product_supplier_dict['ShipmentFreq'] = str(random_integers(min_ordering_frequency, max_ordering_frequency)) + " days"
                    product_supplier_dict['ServiceLevel'] = uniform(min_service_level, max_service_level)

                    supplier_dict['Products'].append(product_supplier_dict)

            product_supplier.append(supplier_dict)

        return product_supplier


    # Create static data: definitions of stores, storage spaces, products and suppliers 
    def __init__(self):
        self.hierarchy = {}
        self.hierarchy['InitialWeeksToSimulate'] = n_weeks_to_simulate
        self.hierarchy['WeeksToForecast'] = n_weeks_to_forecast
        self.hierarchy['RawDataFolder'] = raw_data_folder
        self.hierarchy['PublicParametersFolder'] = public_parameters_folder
        self.hierarchy['PrivateParametersFolder'] = private_parameters_folder        
        self.hierarchy['OrdersFolder'] = orders_folder

        load_hierarchy_from_adl = self.connect_to_adl()
        if load_hierarchy_from_adl:
            # this is not the first run of simulator, load static data from JSON file
            self.load_hierarchy()
            #self.hierarchy['InitialDate'] = (datetime.strptime(self.hierarchy['LastDate'], "%Y-%m-%d %H:%M:%S") + dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            self.hierarchy['InitialDate'] = self.hierarchy['LastDate']
            self.hierarchy['LastDate'] = today_date
            self.store_hierarchy()  # writes the JSON file to blob storage for later access
            self.write_csv_attributes()  # writes the same information in the original CSV form
            print('Loaded existing hierarchy.')

        multithread.ADLDownloader(self.adl, lpath='.\Configurations.xlsx', 
                                  rpath=configuration_folder + '/Configurations.xlsx', overwrite=True)
        self.configurations = pd.read_excel('.\Configurations.xlsx', sheetname='InventoryPolicyConfig')

        if load_hierarchy_from_adl:
            return

        # this is the first run of simulator, initialize static data
        self.hierarchy['InitialDate'] = (datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S") - dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        self.hierarchy['LastDate'] = today_date

        self.__define_brands_products()
        self.__define_suppliers()
 
        # definition of stores
        self.hierarchy['Stores'] = []
        for StoreID in range(1, n_stores + 1):

            # definition of store
            AvgHouseholdIncome, AvgTraffic = np.random.multivariate_normal(mean=[5E4, 100],
                                                                           cov=[[1E8, -1E4],
                                                                                [-1E4, 100]]).tolist()
            store_dict = {}
            store_dict['StoreID'] = str(StoreID)
            store_dict['StoreName'] = 'Store ' + store_dict['StoreID']
            store_dict['AvgHouseholdIncome'] = AvgHouseholdIncome
            store_dict['AvgTraffic'] = AvgTraffic
            store_dict['LossRate'] = loss_rate

            # definition of storage in the store
            store_dict['Storage'] = self.__store_storage()

            # definition of departments in the store
            store_dict['Departments'] = self.__store_departments()
           
            # definition of placements of products in storage space
            store_dict['ProductStorage'] = self.__store_product_storage()
           
            # definitions of suppliers of products
            store_dict['ProductSupplier'] = self.__store_product_supplier()

            for department in store_dict['Departments']:
                for brand in department['Brands']:
                    brands_per_supplier = n_brands / n_suppliers     # we assume that each supplier supplies the same number of brands
                    supplier_index = int((int(brand['BrandID']) - 1) / brands_per_supplier)
                    brand_index = int((int(brand['BrandID']) - 1) % brands_per_supplier)
                    start_index = brand_index * products_per_brand
                    end_index = (brand_index + 1) * products_per_brand
                    if len(brand['Products']) == 1:
                        products = [store_dict['ProductSupplier'][supplier_index]['Products'][start_index]]
                    else:
                        products = store_dict['ProductSupplier'][supplier_index]['Products'][start_index:end_index]
                    for product, product_supplier in zip(brand['Products'], products): 
                        product['MSRP'] = uniform(min_msrp_multiplier, max_msrp_multiplier) * product_supplier['Cost'] 
                        if product['ShelfLife'] == '10000 days':              
                            product['DisposalCost'] = 0
                        else:
                            product['DisposalCost'] = uniform(min_disposal_multiplier, max_disposal_multiplier) * product_supplier['Cost']

            self.hierarchy['Stores'].append(store_dict)

        self.store_hierarchy()  # writes the JSON file to blob storage for later access
        self.write_csv_attributes()  # writes the same information in the original CSV form
        print('Generated new hierarchy (did not load from file).')


    def connect_to_adl(self):
        ''' Connects to ADL, creates main folders, and checks whether hierarchy file already exists '''
        try:
            token = lib.auth(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
            # token = os.environ['AuthorizationToken']
            self.adl = core.AzureDLFileSystem(token=token, store_name=adl_name)
        except Exception as e:
            raise Exception('Error while attempting to connect to Azure Data Lake Store:\n{}'.format(e))

        directories = ['RawDataFolder', 'PublicParametersFolder', 'PrivateParametersFolder']
        for directory in directories:
            if not self.adl.exists(self.hierarchy[directory]):
                try: 
                    self.adl.mkdir(self.hierarchy[directory])
                except Exception as e:
                    raise Exception('Error while attempting to create folder {} in ADL Store {}\n'.format(directory,e)) 

        return (self.adl.exists('{}/{}'.format(self.hierarchy['PrivateParametersFolder'], hierarchy_file)))

        
    def load_hierarchy(self):
        ''' Reloads a JSON-formatted hierarchy '''
        with self.adl.open('{}/{}'.format(self.hierarchy['PrivateParametersFolder'], hierarchy_file),
                           blocksize=2 ** 20) as f:
            self.hierarchy = json.loads(f.read().decode('utf-8'))


    def store_hierarchy(self):
        ''' Stores a JSON-formatted hierarchy (both private and public versions) '''
        with self.adl.open('{}/{}'.format(self.hierarchy['PrivateParametersFolder'], hierarchy_file), 'wb') as f:
            hierarchy_string = json.dumps(self.hierarchy, sort_keys=True, indent=4, separators=(',', ': '))
            f.write(hierarchy_string.encode('utf-8'))

        reduced_hierarchy = deepcopy(self.hierarchy)
        for store in reduced_hierarchy['Stores']:
            del store['LossRate']
            for department in store['Departments']:
                del department['PriceElasticity']
                for brand in department['Brands']:
                    del brand['Desirability']
        
        for brand in reduced_hierarchy['Brands']:
            del brand['Desirability']
                
        with self.adl.open('{}/{}'.format(self.hierarchy['PublicParametersFolder'], hierarchy_file), 'wb') as f:
            hierarchy_string = json.dumps(reduced_hierarchy, sort_keys=True, indent=4, separators=(',', ': '))
            f.write(hierarchy_string.encode('utf-8'))


    #  Write public parameters to CSV for parsing/backward compatibility with older versions
    def write_csv_attributes(self):
        
        file_name = self.hierarchy['PublicParametersFolder'] + '/'
        write_data(((row,) for row in self.hierarchy['Stores']), ['StoreID', 'StoreName', 'AvgHouseholdIncome', 'AvgTraffic'], 
                   self.adl, file_name + 'stores.csv')
        write_data(((brand,) for brand in self.hierarchy['Brands']), ['BrandID', 'BrandName'], self.adl, file_name + 'brands.csv')
        write_data(((supplier,) for supplier in self.hierarchy['Suppliers']), 
                   ['SupplierID', 'SupplierName', 'ShippingCost', 'MinShippingVolume', 'MaxShippingVolume', 'FixedOrderSize', 'PurchaseCostBudget'], 
                   self.adl, file_name + 'suppliers.csv')
        write_data(((brand, product) for brand in self.hierarchy['Brands'] for product in brand['Products']),
                   ['BrandID', 'ProductID', 'ProductName', 'MSRP', 'ProductVolume', 'ShelfLife'], self.adl, file_name + 'brands_products.csv') 
        write_data(((store, department) for store in self.hierarchy['Stores'] for department in store['Departments']),
                   ['StoreID','DepartmentID','DepartmentName'], self.adl, file_name + 'store_departments.csv') 
        write_data(((store, storage) for store in self.hierarchy['Stores'] for storage in store['Storage']),
                   ['StoreID', 'StorageID', 'StorageName', 'StorageVolume', 'StorageCostBudget'], self.adl, file_name + 'store_storage.csv') 
        write_data(((store, storage, product) for store in self.hierarchy['Stores'] for storage in store['ProductStorage'] for product in storage['Products']),
                   ['StoreID', 'StorageID', 'ProductID', 'StorageCost', 'MissedSaleCost', 'MinInventorySize', 'MaxInventorySize'], 
                   self.adl, file_name + 'store_product_storage.csv') 
        write_data(((store, supplier, product) for store in self.hierarchy['Stores'] for supplier in store['ProductSupplier'] for product in supplier['Products']),
                   ['StoreID', 'SupplierID', 'ProductID', 'LeadTime', 'LeadTimeConfidenceInterval', 'MinOrderQuantity', 'MaxOrderQuantity',
                    'QuantityMultiplier', 'Cost', 'BackorderCost', 'ShippingCost', 'PurchaseCostBudget', 'ShipmentFreq', 'ServiceLevel'], 
                   self.adl, file_name + 'store_product_supplier.csv')
        write_data(((store, department, brand, product) for store in self.hierarchy['Stores'] for department in store['Departments']
                                                        for brand in department['Brands'] for product in brand['Products']),
                   ['StoreID', 'DepartmentID', 'BrandID', 'ProductID', 'MSRP', 'DisposalCost'], 
                   self.adl, file_name + 'store_department_brand_products.csv')
       

    # Load all features for all products as a data frame 
    # We assume that a product is supplied by a single supplier
    def get_product_features(self):
        
        features = []
        for store in self.hierarchy['Stores']:
            for department in store['Departments']:
                for brand in department['Brands']:
                    for product in brand['Products']:
                        features.append([store['StoreID'], store['AvgHouseholdIncome'], store['AvgTraffic'], department['DepartmentID'], 
                                         department['PriceElasticity'], brand['BrandID'], brand['Desirability'], product['ProductID'], 
                                         product['MSRP'], store['LossRate'], product['ShelfLife']])
        feature_df = pd.DataFrame(features, columns=['StoreID', 'AvgHouseholdIncome', 'AvgTraffic',
                                                     'DepartmentID', 'PriceElasticity', 'BrandID', 'Desirability',
                                                     'ProductID', 'MSRP', 'LossRate', 'ShelfLife'])

        features_suppliers = []
        for store in self.hierarchy['Stores']:
            for supplier in store['ProductSupplier']:
                for product in supplier['Products']:
                    features_suppliers.append([store['StoreID'], product['ProductID'], supplier['SupplierID'], product['Cost'], product['ShipmentFreq'], 
                                               product['MinOrderQuantity'], product['MaxOrderQuantity'], product['QuantityMultiplier'],
                                               product['LeadTime'], product['LeadTimeConfidenceInterval']])
        feature_supplier_df = pd.DataFrame(features_suppliers, columns = ['StoreID', 'ProductID', 'SupplierID', 'Cost', 'ShipmentFreq', 'MinOrderQuantity',
                                                                          'MaxOrderQuantity', 'QuantityMultiplier', 'LeadTime', 'LeadTimeConfidenceInterval'])

        self.feature_df = pd.merge(feature_df, feature_supplier_df, how = 'inner', on = ['StoreID', 'ProductID'])

        
    def get_prices(self):
        ''' Load/generate prices for each product-store-date combination needed '''
        self.get_product_features()
        
        # We need to do a full outer join between price change dates and product features.
        # Create a dummy column called "ones" for this purpose, and remove it afterward.

        # TBD - change this code to compute prices and demand only for one day. Read prices and demand from previous days

        dates = [pd.to_datetime(self.hierarchy['InitialDate']) + pd.to_timedelta('{} days'.format(i)) for i in
                    range((pd.to_datetime(self.hierarchy['LastDate']) - pd.to_datetime(self.hierarchy['InitialDate'])).days
                        + 7*self.hierarchy['WeeksToForecast'])]
        price_change_df = pd.DataFrame(dates, columns=['DateTime'])
        price_change_df['ones'] = 1
        feature_df = self.feature_df.copy(deep=True)
        feature_df['ones'] = 1
        price_change_df = feature_df.merge(price_change_df, on='ones', how='outer')
        price_change_df.drop('ones', axis=1, inplace=True)
        price_change_df['Price'] = np.NaN  # all prices must be randomly generated since none were loaded
        print('Did not load any suggested prices.')

        price_change_df.Price = price_change_df.apply(self.choose_new_price, axis=1)
        self.price_change_df = price_change_df[['ProductID', 'StoreID', 'DateTime', 'Price']]
        self.store_prices(pd.to_datetime(self.hierarchy['LastDate']))
       

    def choose_new_price(self, row):
        ''' Pick a random price between a product's cost and MSRP '''
        if not np.isnan(row.Price):
            return (row.Price)
        mu = row.Cost + 0.8 * (row.MSRP - row.Cost)
        sd = (0.5 * (row.MSRP - row.Cost)) ** 2
        result = np.random.normal(loc=mu, scale=sd)
        while (result > row.MSRP) or (result < row.Cost):
            result = np.random.normal(loc=mu, scale=sd)
        return (round(result, 2))


    def store_prices(self, last_timestamp):
        ''' Write one price change JSON file per store/date combination '''
        historic_prices = self.price_change_df[self.price_change_df['DateTime'] < last_timestamp]
        for record in historic_prices.groupby(['StoreID', 'DateTime']):
            file_name = '{}/pc_store{}_{}.json'.format(self.hierarchy['RawDataFolder'],
                                                       record[0][0],
                                                       record[0][1].strftime('%Y_%m_%d_%H'))

            ''' Create a dictionary of info to be encoded in JSON format '''
            price_change_dict = {}
            price_change_dict['StoreID'] = int(record[0][0])
            price_change_dict['PriceDate'] = str(record[0][1])
            entries = []
            for row in record[1].itertuples():
                entry_dict = {}
                entry_dict['ProductID'] = str(row.ProductID)
                entry_dict['Price'] = float(row.Price)
                entries.append(entry_dict)
            price_change_dict['PriceUpdates'] = entries

            with self.adl.open(file_name, 'wb') as f:
                price_change_string = json.dumps(price_change_dict, sort_keys=True, indent=4, separators=(',', ': '))
                f.write(price_change_string.encode('utf-8'))


    def get_demand(self):

        demand_columns = ['ProductID', 'StoreID', 'DateTime', 'Price', 'LossRate', 'ShelfLife', 'SupplierID', 'LeadTime', 
                          'LeadTimeConfidenceInterval', 'ShipmentFreq',
                          'MinOrderQuantity', 'MaxOrderQuantity', 'QuantityMultiplier', 'Demand']

        ''' Calculates demand values (currently based on a modified formula suggested by Yiyu) '''
        demand_df = self.price_change_df.merge(self.feature_df, on=['ProductID', 'StoreID'], how='left')
        demand_df['RelativePrice'] = demand_df['Price'] / demand_df.groupby('DepartmentID')['Price'].transform('mean')
        demand_df['FracDiscountOverMSRP'] = (demand_df.MSRP - demand_df.Price) / demand_df.MSRP
        demand_df['Demand'] = demand_df.AvgTraffic * demand_df.Desirability / (1 - demand_df.FracDiscountOverMSRP)
        demand_df.Demand += (demand_df.RelativePrice - 1) * demand_df.Price * demand_df.PriceElasticity
        demand_df.Demand /= demand_df.RelativePrice ** 2
        demand_df.Demand = demand_df.Demand.apply(lambda x: max(x, 5))
        demand_df = demand_df[demand_columns]

        # read previously computed demand
        yesterday = datetime.strftime(pd.to_datetime(today_date) -  dt.timedelta(days = 1),"%Y-%m-%d_%H_%M_%S")
        previous_demand_columns = ['StoreID','ProductID','DateTime','Demand','PredictedDemandDistribution','PredictedDemandVariance','PredictedDemandProbability']
        previous_demand_df = pd.DataFrame(columns = previous_demand_columns)
        for partition, group in demand_df.groupby(['StoreID', 'ProductID']):
            file_name = '{}/demand_forecasts/{}/{}/{}.csv'.format(self.hierarchy['RawDataFolder'], partition[0], partition[1], 
                        yesterday)     
            if self.adl.exists(file_name):    
                with self.adl.open(file_name, blocksize=2 ** 20) as f:
                    previous_demand = pd.read_csv(StringIO(f.read().decode('utf-8')), sep=",", dtype={'StoreID': str}, parse_dates = ['DateTime'], header = 0,
                                                  names = previous_demand_columns)
                    previous_demand_df = pd.concat([previous_demand_df, previous_demand], ignore_index = True)

        # merge previous and new demand. when we have both new and old value of demand, take the ond one
        merged = pd.merge(demand_df, previous_demand_df, how='left', on=['StoreID', 'ProductID', 'DateTime'], suffixes = ['','_prev'])
        merged['Demand'] = merged.apply(lambda x: x['Demand'] if np.isnan(x['Demand_prev']) else x['Demand_prev'], axis = 1)
        
        self.demand_df = merged[demand_columns]

        # store future demand values in CSV file
        demand_csv = self.demand_df[['StoreID', 'ProductID','DateTime','Demand']][demand_df['DateTime'] >= pd.to_datetime(today_date)]
        demand_csv.rename({'Demand': 'PredictedDemand'}, inplace=True)
        demand_csv['PredictedDemandDistribution'] = ''
        demand_csv['PredictedDemandVariance'] = -1    
        demand_csv['PredictedDemandProbability'] = 1       

        # write predicted demand to CSV files, one file per store, product 
        for partition, group in  demand_csv.groupby(['StoreID', 'ProductID']):
            file_name = '{}/demand_forecasts/{}/{}/{}.csv'.format(self.hierarchy['RawDataFolder'], partition[0], partition[1], 
                        today_date.replace(' ','_').replace(':','_'))
            with self.adl.open(file_name, 'wb') as f:
                f.write(group.to_csv(index=False).encode('utf-8'))


class Store:
    ''' Simulates sales of all products in a given store '''

    def __init__(self, description, store_id):
        ''' Create a new store given the full simulation AttributeDescription and StoreID '''
        self.description = description
        self.adl = description.adl
        self.folder = description.hierarchy['RawDataFolder']
        self.store_id = store_id

        '''
        Workday length and operating time is not currently a tunable parameter.
        Opening hours are hard-coded as 7 AM - 9 PM, seven days a week
        '''
        self.workday_length = 14. / 24
        self.opening_time = pd.to_timedelta('7 hours')
        self.closing_time = pd.to_timedelta('21 hours')
        self.todays_sales = []

        # Load orders
        self.orders = pd.DataFrame(columns=('PolicyName', 'StoreID', 'ProductID', 'SupplierID', 'Quantity', 
                                            'OrderTimestamp', 'ETA', 'ConfidenceInterval', 'Fulfilled'))

        if self.adl.exists(description.hierarchy['OrdersFolder']):
            policies = self.adl.ls(description.hierarchy['OrdersFolder'])
            for policy in policies:
                file_name = '{}/orders_{}.csv'.format(policy, self.store_id)
                if description.adl.exists(file_name):
                    with description.adl.open(file_name, blocksize=2 ** 20) as f:
                        policy_orders = pd.read_csv(StringIO(f.read().decode('utf-8')), header = 0, sep=",", parse_dates=['OrderTimestamp', 'ETA'],
                                                    names=['PolicyName', 'StoreID', 'ProductID', 'SupplierID', 'Quantity', 
                                                           'OrderTimestamp', 'ETA', 'ConfidenceInterval', 'Fulfilled'],
                                                    dtype={'PolicyName':str, 'StoreID':str, 'ProductID':str, 
                                                           'SupplierID':str, 'Quantity':int, 'ConfidenceInterval':int, 'Fulfilled':bool})
                        self.orders = pd.concat([self.orders, policy_orders])
                    self.orders = self.orders.reset_index().iloc[:,1:]
 
        ''' Create an inventory for each product. Created inventory = predicted demand '''
        self.demand_df = description.demand_df.loc[description.demand_df.StoreID == store_id]
        self.product_ids = self.demand_df['ProductID'].unique()
        first_date = self.demand_df['DateTime'].min()
        self.inventories = {}
        for product_id in self.product_ids:
            row = self.demand_df.loc[(self.demand_df['ProductID'] == product_id) &
                                     (self.demand_df['DateTime'] == first_date)]
            self.inventories[product_id] = Inventory(description, row.iloc[0], self.orders)
        self.backorders = {}
       

    def run(self):
        ''' Iterate through the dates, generating sales and loss events '''
        conversion_factor = self.workday_length # was 7 * workday_length
        for StartDate, date_df in self.demand_df.groupby('DateTime', sort=True):
            ''' Find the sales and loss rates for each product in that week '''
            if StartDate >= pd.to_datetime(today_date):
                break
            
            print(StartDate)
            rates = []
            events = []
            for row in date_df.itertuples():
                self.inventories[row.ProductID].update_price(row.Price)
                rates.extend([row.Demand, row.LossRate])
                events.extend([[row.ProductID, True], [row.ProductID, False]])

            ''' Beta is the expectation of inverse time until another event occurs '''
            beta = conversion_factor / sum(rates)
            rate_ids = list(range(len(rates)))
            probabilities = [i / sum(rates) for i in rates]

            # iterate over 1 day (TBD - simplify this code)
            for i in range(1):
                my_start_date = StartDate + i * pd.to_timedelta('1 days') + self.opening_time
                if my_start_date > pd.to_datetime(today_date):
                    break
                workday_elapsed = 0.
                self.todays_backorders = []
                self.get_deliveries(my_start_date) # get deliveries of orders
                
                while True:
                    ''' Choose a time elapsed until the next event '''
                    workday_elapsed += np.random.exponential(scale=beta)
                    if workday_elapsed > self.workday_length:
                        break

                    ''' Choose which event occurred at that time and attempt it '''
                    event_id = np.random.choice(a=rate_ids, p=probabilities)
                    product_id, is_sale = events[event_id]
                    result = self.inventories[product_id].remove_unit(is_sale=is_sale,
                                                                      time=my_start_date + pd.to_timedelta(workday_elapsed, unit='d'))

                    ''' If a sale was successfully attempted (the item was in stock), record the sale '''
                    if is_sale:
                        if result is not None:
                            self.todays_sales.append(result)
                        #else:
                        # backorder item, it was not in the inventory. 
                        # TBD in the future
                        #   - maintain inventory per policy
                        #   - fulfill orders from all policies
                        #   - generate multiple sales, one from each policy
                        #   - export only sales of active policies 
                        #   - export only inventory of active policies
                        self.todays_backorders.append(product_id)

                self.end_of_day(my_start_date)
       

    # fulfill orders from active policies
    def get_deliveries(self, current_date):
        configuration = self.description.configurations
        
        for i, order in self.orders.iterrows():
            # check if this order can be delivered today
            active = configuration.loc[configuration['InventoryPolicyName'] == str(order['PolicyName']),'ActiveFlag'].iat[0]
            if active == 1 and order['ETA'].date() == current_date.date() and order['Fulfilled'] == False:
                self.orders.at[i,'Fulfilled'] = True
                # add inventory record
                brand_id = int(order['ProductID'].split('_')[0]) - 1
                shelf_life = int(self.description.hierarchy['Brands'][brand_id]['Products'][0]['ShelfLife'].split(' ')[0])
                self.inventories[order['ProductID']].add_inventory(current_date + shelf_life * pd.to_timedelta('1 days'), 
                                                                   order['Quantity'])
        

    def poor_mans_zero_truncated_poisson(self, k):
        ''' Draw x>=1 from a Poisson distribution (to determine # of items in a transaction) '''
        result = np.random.poisson(k)
        while (result == 0):
            result = np.random.poisson(k)
        return (result)


    def group_sales_into_transaction(self, sale_list):
        ''' Group individual item sales records into a single receipt '''
        products = []
        subtotal = 0.
        for sale in sale_list:
            entry_dict = {}
            entry_dict['ProductID'] = sale['ProductID']
            entry_dict['Price'] = sale['Price']
            products.append(entry_dict)
            subtotal += sale['Units'] * sale['Price']
        transaction = {}
        transaction['TransactionDateTime'] = sale_list[-1]['TransactionDateTime']
        transaction['Subtotal'] = round(subtotal, 2)
        transaction['Tax'] = round(subtotal * 0.07, 2)
        transaction['Total'] = round(subtotal + transaction['Tax'], 2)
        transaction['Products'] = products
        return (transaction)


    def end_of_day(self, current_date):
        ''' Write out sales transactions and inventory for the day '''
        ''' Begin by writing the inventory summary '''
        inventory_summaries = []
        spoilage_summaries = []
        for product_id in self.product_ids:
            inventory_summary, spoilage_summary = self.inventories[product_id].end_of_day()
            inventory_summaries.append(inventory_summary)
            if len(spoilage_summary['CurrentSpoilages']) > 0:
                spoilage_summaries.append(spoilage_summary)
        write_date = self.inventories[self.product_ids[0]].last_write_date.strftime('%Y-%m-%d %H:%M:%S')
        write_date_file_format = self.inventories[self.product_ids[0]].last_write_date.strftime('%Y_%m_%d_%H_%M_%S')
     
        inventory_dict = {}
        inventory_dict['StoreID'] = int(self.store_id)
        inventory_dict['InventoryDateTime'] = write_date
        inventory_dict['Products'] = inventory_summaries
      
        spoilages_dict = {}
        spoilages_dict['StoreID'] = int(self.store_id)
        spoilages_dict['SpoilageDateTime'] = write_date
        spoilages_dict['Products'] = spoilage_summaries

        # save inventory in JSON format
        inventory_file_name = '{}/inv_store{}_{}.json'.format(self.folder, self.store_id, write_date_file_format)
        with self.adl.open(inventory_file_name, 'wb') as f:
            inventory_string = json.dumps(inventory_dict, sort_keys=True, indent=4, separators=(',', ': '))
            f.write(inventory_string.encode('utf-8'))

        # save inventory in CSV format
        inventory_file_name = '{}/inv_store{}_{}.csv'.format(self.folder, self.store_id, write_date_file_format)
        write_data(((store, product, timestamp, inventory_record) for store in [{'StoreID': self.store_id}]
                                                                  for product in inventory_dict['Products']
                                                                  for timestamp in [{'InventoryDateTime': write_date}]
                                                                  for inventory_record in product['CurrentInventory']),
                   ['StoreID', 'ProductID', 'InventoryDateTime', 'Units', 'ExpiryDateTime'], self.adl, inventory_file_name)

        ''' Now create the sales summary '''
        sales_dict = {}
        sales_dict['StoreID'] = int(self.store_id)
        sales_dict['SalesLogDateTime'] = write_date
        transactions = []
        idx = 0
        while (idx < len(self.todays_sales)):
            n = self.poor_mans_zero_truncated_poisson(2)  # max num of items in next transaction
            transactions.append(self.group_sales_into_transaction(self.todays_sales[idx:min(idx + n,
                                                                                            len(self.todays_sales))]))
            idx += n

        sales_dict['Transactions'] = transactions

        # append spoilages to sales
        if len(spoilage_summaries) > 0:
            rows = []
            for product in spoilages_dict['Products']:
                for spoilage_record in product['CurrentSpoilages']:
                    spoilage_dict = {}
                    spoilage_dict['ProductID'] = product['ProductID']
                    spoilage_dict['TransactionDateTime'] = write_date 
                    spoilage_dict['Units'] = spoilage_record['Units']
                    spoilage_dict['Price'] = 0
                    rows.append(spoilage_dict)

            self.todays_sales.extend(rows)

        # save sales in JSON format
        sales_file_name = '{}/sales_store{}_{}.json'.format(self.folder, self.store_id, write_date_file_format)
        with self.adl.open(sales_file_name, 'wb') as f:
            sales_string = json.dumps(sales_dict, sort_keys=True, indent=4, separators=(',', ': '))
            f.write(sales_string.encode('utf-8'))

        # save sales in CSV format
        sales_file_name = '{}/sales_store{}_{}.csv'.format(self.folder, self.store_id, write_date_file_format)
        write_data(((store, sale_record) for store in [{'StoreID': self.store_id}] for sale_record in self.todays_sales), 
                   ['StoreID', 'ProductID', 'TransactionDateTime', 'Units', 'Price'], self.adl, sales_file_name)

        self.todays_sales = []

        # add today's backorders to the global dataset of backorders
        counts = Counter(self.todays_backorders) 
        # Python 2 - for product, n_backorders in counts.items():
        for product, n_backorders in counts.items():
            if not product in self.backorders:
                self.backorders[product] = n_backorders
            else:
                self.backorders[product] += n_backorders

        # try to place orders on backordered items
        store_id_int = int(self.store_id) - 1
        store_data = self.description.hierarchy['Stores'][store_id_int]
        n_orig_orders = self.orders.shape[0]
        # Python 2 - for product, n_backorders in self.backorders.iteritems():
        for product, n_backorders in self.backorders.items():

            # find supplier data for a product
            product_index = int(product.split('_')[0]) - 1
            products_per_supplier = n_brands / n_suppliers
            supplier_index = int(product_index / products_per_supplier)
            product_index = int(product_index % products_per_supplier)
            supplier_product = store_data['ProductSupplier'][supplier_index]['Products'][product_index]
            supplier_id = store_data['ProductSupplier'][supplier_index]['SupplierID']

            # all supplier constraints are satisfied, we can place an order
            can_order = True
            if supplier_product['MinOrderQuantity'] > n_backorders or supplier_product['QuantityMultiplier'] > n_backorders:
                can_order = False    # wait till we have enough items to order
            else:
                shipment_freq = int(supplier_product['ShipmentFreq'].split(' ')[0])
                if current_date.toordinal() % shipment_freq != 0:
                    can_order = False    # wait until supplier will start to accept the orders
                
            if can_order:
                order_size = n_backorders
                if supplier_product['MaxOrderQuantity'] > 0:
                    order_size = min(n_backorders, supplier_product['MaxOrderQuantity'])
                if supplier_product['QuantityMultiplier'] > 0:
                    order_size = order_size - order_size % supplier_product['QuantityMultiplier']
                self.backorders[product] = n_backorders - order_size
                    
                policy_name = get_policy_name(self.store_id, product, supplier_id)
                self.orders.loc[self.orders.shape[0] + 1] = [policy_name, self.store_id, product, supplier_id, 
                                                             order_size, current_date, 
                                                             current_date + supplier_product['LeadTime'] * pd.to_timedelta('1 days'), 
                                                             supplier_product['LeadTimeConfidenceInterval'], False]

        # clean up the dictionary of backorders
        self.backorders = {product: v for product, v in self.backorders.items() if v > 0}        

        # save orders
        self.orders['Quantity'] = self.orders['Quantity'].astype(int)
        self.orders['ConfidenceInterval'] = self.orders['ConfidenceInterval'].astype(int)
        grouped = self.orders.groupby('PolicyName')
        conf = self.description.configurations

        for name, group in grouped:

            directory_name = conf.loc[conf['InventoryPolicyName'] == str(name),'DirectoryName'].iat[0]
            orders_file_name = self.description.hierarchy['OrdersFolder'] + '/' + str(directory_name) + '/orders_' + str(self.store_id) + '.csv'
            with self.adl.open(orders_file_name, 'wb') as f:
                f.write(group.to_csv(index=False).encode('utf-8'))

            partial_orders_file_name = self.description.hierarchy['OrdersFolder'] + '/' + str(directory_name) + '/partial_orders_' + str(self.store_id) + '.csv'
            if self.adl.exists(partial_orders_file_name):
                if n_orig_orders < self.orders.shape[0]:
                    new_orders_all = self.orders.iloc[n_orig_orders:self.orders.shape[0]]
                    new_orders = new_orders_all[new_orders_all['PolicyName'] == name]
                    with self.adl.open(partial_orders_file_name, 'ab') as f:
                        f.write(new_orders.to_csv(index=False, header=False).encode('utf-8'))
            else:
                with self.adl.open(partial_orders_file_name, 'wb') as f:
                    f.write(group.to_csv(index=False).encode('utf-8'))


class Inventory:
    ''' Maintains product inventory (not limiting in demand forecasting/price optimization solution) '''

    def  __compute_arrivals(self):
         min_order = max(1,self.min_order_quantity)
         if self.max_order_quantity < 0:
             max_order = 10000
         else:
             max_order = min(10000, self.max_order_quantity)
         order_arrival = int(random_integers(min_order, max_order))
         if self.quantity_multiplier > 0:
             return max(min_order, order_arrival - (order_arrival % self.quantity_multiplier))     
         else:
             return order_arrival


    def __init__(self, description, row, orders):
        ''' Load last inventory record if possible; otherwise, create a new inventory '''
        self.store_id = row.StoreID
        self.product_id = row.ProductID
        self.price = row.Price
        self.min_order_quantity = row.MinOrderQuantity
        self.max_order_quantity = row.MaxOrderQuantity
        self.quantity_multiplier = row.QuantityMultiplier
        self.shipment_frequency = pd.to_timedelta(row.ShipmentFreq)
        self.shelf_life = pd.to_timedelta(row.ShelfLife)
        self.inventory = []
        self.arrivals = 0
        self.last_write_date = row.DateTime

        file_name = '{}/inv_store{}_{}.json'.format(description.hierarchy['RawDataFolder'],
                                                    self.store_id,
                                                    row.DateTime.strftime('%Y_%m_%d_%H_%M_%S'))
        if description.adl.exists(file_name):
            with description.adl.open(file_name, blocksize=2 ** 20) as f:
                last_inventory = json.loads(f.read().decode('utf-8'))
                for product in last_inventory['Products']:
                    if product['ProductID'] == self.product_id:
                        break
                for batch in product['CurrentInventory']:
                    self.inventory.append([pd.to_datetime(batch['ExpiryDateTime']), batch['Units']])
        else:
            self.start_date = row.DateTime
            self.arrivals = self.__compute_arrivals()  

        self.sales = 0
        self.losses = 0
        self.spoilages = 0
        

    def update_price(self, price):
        ''' Used to update the price included in sales records '''
        self.price = price
        

    def remove_unit(self, time, is_sale=True):
        ''' Checks whether a new sale/loss event is possible, and if so, updates inventory and sale records '''
        time = time.round(freq='1s')
        if len(self.inventory) > 0:
            if is_sale:
                self.sales += 1
            else:
                self.losses += 1
            if self.inventory[0][1] == 1:
                self.inventory.pop(0)  # last item from this expiry date; remove its entry
            else:
                self.inventory[0][1] -= 1
             
            event_dict = {}
            if is_sale:
                ''' Create a JSONable description of a sale (no need for loss events) '''
                event_dict['TransactionDateTime'] = str(time)
                event_dict['ProductID'] = self.product_id
                event_dict['Units'] = 1
                event_dict['Price'] = round(float(self.price), 2)
            return event_dict
        else:
            return None  # this return value indicates this item is sold out


    def end_of_day(self):
        ''' Remove expired products, write inventory record, and reset daily event tallies '''
        time_elapsed = pd.to_timedelta('1 days')
        current_write_date = self.last_write_date + time_elapsed
        self.last_write_date = current_write_date

        ''' Remove any products now expired '''
        self.spoilages = sum([i[1] for i in self.inventory if i[0] <= self.last_write_date])
        self.spoilages_details = [i for i in self.inventory if i[0] <= self.last_write_date] 
        self.inventory = [i for i in self.inventory if i[0] > self.last_write_date]

        if len(self.spoilages_details) > 0:
            neg = [u[1] for u in self.spoilages_details if u[1] <= 0]

        spoilages_summary = self.write_spoilage_summary()

        ''' Write and reset sale/loss/arrival tallies '''
        inventory_summary = self.write_inventory_summary()
        self.arrivals = 0
        self.sales = 0
        self.losses = 0
        self.spoilages = 0
        return inventory_summary, spoilages_summary


    def add_inventory(self, expiration_date, units):
        self.inventory.append([expiration_date, units])
           

    def write_spoilage_summary(self):
        spoilage_dict = {}
        spoilage_dict['ProductID'] = str(self.product_id)
        current_spoilages = []
        for i in self.spoilages_details:
            entry_dict = {}
            entry_dict['ExpiryDateTime'] = str(i[0])
            entry_dict['Units'] = int(i[1])
            current_spoilages.append(entry_dict)
        spoilage_dict['CurrentSpoilages'] = current_spoilages
        return spoilage_dict


    def write_inventory_summary(self):
        ''' Create a JSONable description of the inventory on this date '''
        inventory_dict = {}
        inventory_dict['ProductID'] = str(self.product_id)
        inventory_dict['Arrivals'] = int(self.arrivals)
        inventory_dict['Sales'] = int(self.sales)
        inventory_dict['Losses'] = int(self.losses)
        inventory_dict['Spoilages'] = int(self.spoilages)
        current_inventory = []
        for i in self.inventory:
            entry_dict = {}
            entry_dict['ExpiryDateTime'] = str(i[0])
            entry_dict['Units'] = int(i[1])
            current_inventory.append(entry_dict)
        inventory_dict['CurrentInventory'] = current_inventory
        return inventory_dict


if __name__ == '__main__':

    # define static data
    description = AttributeDescription()
    description.get_prices()
    description.get_demand()
    store_ids = description.demand_df['StoreID'].unique()
    for store_id in store_ids:
        print(store_id)
        my_store = Store(description, store_id)
        my_store.run()
