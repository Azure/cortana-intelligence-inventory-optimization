# Inventory Optimization - A Cortana Intelligence Solution How-To Guide

## Abstract
This **Automated Deployment Guide** contains the post-deployment instructions for the deployable **Inventory Optimization for Retail** solution in the Cortana Intelligence Gallery. 

<Guide type="PostDeploymentGuidance" url="https://github.com/Azure/cortana-intelligence-inventory-optimization/blob/master/Automated%20Deployment%20Guide/Post%20Deployment%20Instructions.md"/>

## Summary
<Guide type="Summary">
Inventory management is one of the central problems in retail. Frequently inventory managers need to decide how many items of each product they need to order from suppliers. A manual ordering of products cannot scale to thousands of products and cannot take into account changing demands and many business constraints and costs. Existing inventory optimization systems are not scalable enough to meet the requirements of large retailers. Also, these systems are not flexible enough and cannot incorporate important business goals and constraints.

In this Solution How-To Guide, we develop a cloud-based, scalable, and flexible inventory optimization solution. To scale up for hundreds of thousands of store and product combinations,  we use Azure Data Lake Analytics for data processing and Azure Batch for solving optimization problems in parallel. We provide scripts for eight commonly used inventory optimization policies. These scripts can be customized for a specific retailer and new policies can be added by providing a few scripts. We included Bonmin, an open-source solver for general MINLP (Mixed Integer NonLinear Programming) problems, in a Docker image. Additional open-source solvers (e.g. MIPCL) and commercial solvers like Gurobi can be easily incorporated into this Docker image. For details of the inventory policies included and instructions on how to customize this solution, please refer to the TechnicalGuide.pdf.

Data scientists and developers will tailor this solution to business goals and constraints of big retailers and will build custom large-scale inventory optimization systems on top of it. These systems will speed up the ordering process and will improve widely used inventory management business metrics (e.g. normalized revenue per day and inventory turnover). 
</Guide>

## Prerequisites
<Guide type="Prerequisites">

- This pattern requires creation of **1 Azure Batch Account**, **1 DataLake Analytics** and **1 DataLake Store**. Ensure adequate Azure Batch, DataLake Analytics and DataLake Stores are available before provisioning. By default, subscriptions each region has 3 Azure Batch accounts. The limit can be increased. Please consider deleting any unused Azure Batch and DataLake from your subscription. You may contact [Azure Support](https://azure.microsoft.com/support/faq/) if you need to increase the limit.

- This pattern requires user to have admin or owner privilege in order to create Service Principal in later steps during solution deployment. Check your account permissions using the document [Required permissions](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#required-permissions). 
</Guide>

## Description

#### Estimated Provisioning Time: <Guide type="EstimatedTime">1 Hour</Guide>
<Guide type="Description">
The Cortana Intelligence Suite provides advanced analytics tools through Microsoft Azure — data ingestion, data storage, data processing and advanced analytics components — all of the essential elements for building an inventory optimization for retail solution.

The 'Deploy' button will launch a workflow that will deploy an instance of the solution within a Resource Group in the Azure subscription you specify. The solution includes multiple Azure services (described below) along with a web job that simulates data so that immediately after deployment you have a working end-to-end solution. 

## Solution Architecture
In this section, we provide more details about how the above solution is operationalized in Cortana Intelligence Suite. The figure below describes the solution architecture.

![](https://github.com/Azure/cortana-intelligence-inventory-optimization/blob/master/Manual%20Deployment%20Guide/Figures/SolutionArchitecture.png)

### What's Under the Hood
- **Data source**: The data in this solution is generated using a data simulator, including demand forecasting, inventory level, and sales data. These simulated data are saved on Azure Data Lake Store. 
- **Data pre-processing**: The raw data is first converted to [Pyomo](http://www.pyomo.org/) input format using Azure Data Lake Analytics (ADLA) U-SQL jobs. Then Pyomo python script converts the input into standard optimization problem formats, .nl or .mps. 
- **Parallel optimization using Azure Batch**: The optimization problems are solved by BONMIN in Docker containers. We create a task for each data partition, e.g. store and product combination, and all the tasks are executed in parallell in an Azure Batch virtual machine pool.
- **Result post-processing**: The results of solving optimization problems are converted to order time and order amount by ADLA U-SQL jobs. Both intermediate and final results are saved on Azure Data Lake Store
- **Orchestration and schedule**: A **Main** Azure Web Job is scheduled to run once every hour. This web job invokes the other web jobs that are executed according to the schedule of each inventory policy in an excel configuration file. 
- **Visualize**: A PowerBI Dashboard is used to visualize inventory policy performance and inventory level. 

</Guide>
##### Disclaimer
©2017 Microsoft Corporation. All rights reserved.  This information is provided "as-is" and may change without notice. Microsoft makes no warranties, express or implied, with respect to the information provided here.  Third party data was used to generate the solution.  You are responsible for respecting the rights of others, including procuring and complying with relevant licenses in order to create similar datasets.
