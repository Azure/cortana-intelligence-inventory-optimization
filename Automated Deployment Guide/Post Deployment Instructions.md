# Inventory Optimization for Retail 

## Abstract
This document is focusing on the post deployment instructions for the automated deployment through [Cortana Intelligence Solutions](https://gallery.cortanaintelligence.com/solutions). The source code of the solution as well as manual deployment instructions can be found [here](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide).

### Quick links
[Monitor Progress](#monitor-progress) - See how you can monitor the resources that have been deployed to your subscription.

[Visualization Steps](#visualization) - Instructions to connect up a Power BI dashboard to your deployment that visualized the results.

[Scaling](#scaling) - Guidance on how to think about scaling this solution according to your needs.

[Customization](#customization) - Guidance on how to think about customizing this solution with your own data.



## Monitor Progress
Once the solution is deployed to the subscription, you can see the services deployed by clicking the resource group name on the final deployment screen in the CIS.

This will show all the resources under this resource groups on [Azure management portal](https://portal.azure.com/).

After successful deployment, the entire solution is automatically started on cloud. You can monitor the progress from the following resources.
This part contains instructions for managing different Azure componenets in the deployed solution.

### Azure Function
Azure Function app is created during the deployment. This hosts 7 webjobs which performs mostly orchastration and data simulation. You can monitor the webjob by clicking the link on your deployment page. 

The inventory optimization solution is composed of five major components: data simulation, data pre-processing, inventory optimization, order placement, and evaluation. The table below lists the task and execution engine of each component. Each step starts with an Azure Web Job. The actual execution is done by Python scripts within the web jobs, Azure Data Lake Analytics jobs submitted by the web job Python script, or Azure Batch jobs submitted by the web job Python script. 

|Web Job Name	| Task |	Execution Engine|
|------------------------|---------------------|---------------------|
| Simulator              |Simulate raw data: stores, storage spaces, products, suppliers, demand forecasting, sales, and inventory levels, order placements and deliveries.| Python script in the web job   |
| InventoryOptimization(Data extraction part)  | Use Pyomo to define abstract optimization problems of inventory management policies.Extract known values of optimization problems from the raw data. | Pyomo Python scripts in the web job. U-SQL jobs on Azure Data Lake Analytics  |
| InventoryOptimization(Optimization part)      |Create inventory management policy by solving inventory optimization problems using Bonmin. | Azure Batch   |
| GenerateOrder             |Use solution optimization problem, current time and inventory levels to place new orders.| U-SQL jobs on Azure Data Lake Analytics  |
| Evaluation             |Compute performance metrics of inventory management policies.|  Python script in the web job |
| UploadScriptToADLS             |Upload the configuration files/scripts for optimization and runs UploadStaticDataToADLS which uploads the static data for initial PowerBI reports.| Python script in the web job |
| UploadStaticDataToADLS             |Uploads the static data to Azure DataLake Store.|  Python script in the web job |
| InstallPackages             | Installs required python packages on Azure Functions Server to run above mentioned webjobs.|  Bash script in the web job |


> **Note**: For demo purpose, a master webjob(Main) is scheduled to run every hour and invokes the other webjobs to simulate one day every hour. The figure below illustrates the data flow between different webjobs. Note that all web jobs write/read to/from Azure Data Lake Store (ADLS). 

![](https://github.com/Azure/cortana-intelligence-inventory-optimization/blob/master/Manual%20Deployment%20Guide/Figures/webJobFlow.png)

### Azure Data Lake Store
Both raw data and analytical results are saved in **Azure Data Lake Store** in this solution. You can monitor the generated datasets by clicking the link on your deployment page. Detailed description of the data in Azure DataLake store can be found in [Technical Deployment Guide](https://github.com/Azure/cortana-intelligence-resource-optimization) under section **Structure of Directories in Azure DataLake**


### Setup Power BI

- The essential goal of this part is to visualize the results from the inventory optimization solution. Power BI can directly connect to the DataLake Store, where the results are stored.

> Note: In this step, the prerequisite is to download and install the free software [Power BI desktop](https://powerbi.microsoft.com/desktop). We recommend you start this process 2-3 hours after you finish deploying the solution so that you have more data points to visualize.

- You can follow the instructions under the section of **'Setup Power BI'** in this [document](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide#11-set-up-powerbi-dashboard) to create your own dashboard.

## Scaling

The architecture of this solution is designed to be scalable. Azure DataLake Store can scale throughput to support any size of analytic workload without redesigning your application or repartitioning your data at higher scale. Azure Batch, can also be scaled up to fit into heavier computation needs.

## Customization

For solution customization, you can refer to the [Technical Deployment Guide](https://github.com/Azure/cortana-intelligence-inventory-optimization) and [Manual Deployment Guide](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide) to gain an inside view of how the solution is built, the function of each component and access to all the source codes used in the demo solution


##### Disclaimer
©2016 Microsoft Corporation. All rights reserved.  This information is provided "as-is" and may change without notice. Microsoft makes no warranties, express or implied, with respect to the information provided here.  Third party data was used to generate the solution.  You are responsible for respecting the rights of others, including procuring and complying with relevant licenses in order to create similar datasets.
