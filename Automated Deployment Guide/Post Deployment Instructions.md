# Inventory Optimization for Retail 

## Abstract
This document is focusing on the post deployment instructions for the automated deployment through [Cortana Intelligence Solutions](https://gallery.cortanaintelligence.com/solutions). The source code of the solution as well as manual deployment instructions can be found [here](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide).

## Monitor Progress
Once the solution is deployed to the subscription, you can see the services deployed by clicking the resource group name on the final deployment screen in the CIS.

This will show all the resources under this resource groups on [Azure management portal](https://portal.azure.com/).

After successful deployment, the entire solution is automatically started on cloud. You can monitor the progress from the following resources.
This part contains instructions for managing different Azure componenets in the deployed solution.
### Web Jobs
A web job is created during the deployment. You can monitor the web job by clicking the link on your deployment page. This web job will generate weekly sales data hourly. The generated sales data will be stored in **Azure Data Lake Store**.
> **Note**: In the demo here, the simulator will generate **one week's** simulated data in **one hour**. And **Azure Data Factory** is scheduled to process, and output the results for **one week's** data **in one hour**. 
 That is to say, in this solution demo, one week is condensed to one hour. In this case, you are able to view multiple weeks' results in a few hours, rather than waiting for multiple weeks to get the results for a few weeks. However, in the reality deployment, the cycle time should be consistent with the real time.

### Azure Data Lake Store
Both raw data and analytical results are saved in **Azure Data Lake Store** in this solution. You can monitor the generated datasets by clicking the link on your deployment page.

There are mainly two final result datasets: **Aggregated Sales Data** and **Optimization Result Data**. Each record of **Aggregated Sales Data** contain weekly sales, product features and store features for one product sold at one store in a specific week. Each record of **Optimization Result Data** contain predicted weekly sales on this record's features, recommended optimal price, product features and store features for one product sold at one store in a specific week. **Aggregated Sales Data** only contain historical data, whereas **Optimization Result Data** contain historical recommendations as well as the future price recommendation for the coming week. **Aggregated Sales Data** contain records for all stores, whereas **Optimization Result Data** only contain records for stores in treatment group, because only stores in treatment group accepts/needs the recommended price from optimization algorithm.

For both **Aggregated Sales Data** and **Optimization Result Data**, the solution produces result datasets in [**Parquet file**](<http://parquet.apache.org/>) format, which is a columnar storage format in the Hadoop ecosystem. The **Parquet files** can be access by sql query, using `%%sql` magic in **Jupyter Notebook** pre-installed on HDinsight Spark Cluster. 

### Setup Power BI

- The essential goal of this part is to visualize the results from the inventory optimization solution. Power BI can directly connect to the DataLake Store, where the results are stored.

> Note: In this step, the prerequisite is to download and install the free software [Power BI desktop](https://powerbi.microsoft.com/desktop). We recommend you start this process 2-3 hours after you finish deploying the solution so that you have more data points to visualize.

- You can follow the instructions under the section of **'Setup Power BI'** in this [document](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide#11-set-up-powerbi-dashboard) to create your own dashboard.

## Scaling

## Customization

##### Disclaimer
©2016 Microsoft Corporation. All rights reserved.  This information is provided "as-is" and may change without notice. Microsoft makes no warranties, express or implied, with respect to the information provided here.  Third party data was used to generate the solution.  You are responsible for respecting the rights of others, including procuring and complying with relevant licenses in order to create similar datasets.
