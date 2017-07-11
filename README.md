# Inventory Optimization - A Cortana Intelligence Solution How-To Guide
Inventory management is one of the central problems in retail. Frequently inventory managers need to decide how many items of each product they need to order from suppliers. A manual ordering of products cannot scale to thousands of products and cannot take into account changing demands and many business constraints and costs. Existing inventory optimization systems are not scalable enough to meet the requirements of large retailers. Also, these systems are not flexible enough and cannot incorporate important business goals and constraints.

In this Solution How-To Guide, we develop a cloud-based, scalable, and flexible inventory optimization solution. To scale up for hundreds of thousands of store and product combinations,  we use [Azure Data Lake Analytics](https://azure.microsoft.com/en-us/services/data-lake-analytics/) for data processing and [Azure Batch](https://azure.microsoft.com/en-us/services/batch/) for solving optimization problems in parallel. We provide scripts for eight commonly used inventory optimization policies. These scripts can be customized for a specific retailer and new policies can be added by providing a few scripts. We included [Bonmin](https://projects.coin-or.org/Bonmin), an open-source solver for general MINLP (Mixed Integer NonLinear Programming) problems, in a Docker image. Additional open-source solvers (e.g. [MIPCL](http://www.mipcl-cpp.appspot.com/)) and commercial solvers like Gurobi can be easily incorporated into this Docker image. For details of the inventory policies included and instructions on how to customize this solution, please refer to the TechnicalGuide.pdf.

Data scientists and developers will tailor this solution to business goals and constraints of big retailers and will build custom large-scale inventory optimization systems on top of it. These systems will speed up the ordering process and will improve widely used inventory management business metrics (e.g. normalized revenue per day and inventory turnover).

## Solution Architecture
In this section, we provide more details about how the above solution is operationalized in Cortana Intelligence Suite. The figure below describes the solution architecture.

![](https://github.com/Azure/cortana-intelligence-inventory-optimization/blob/master/Manual%20Deployment%20Guide/Figures/SolutionArchitecture.png)

### What's Under the Hood
- **Data source**: The data in this solution is generated using a data simulator, including stores, storage spaces, products, suppliers, demand forecasting, sales, inventory levels, and order placements and deliveries data. These simulated data are saved on Azure Data Lake Store.
- **Data pre-processing**: First, extract know values of optimization problems from the raw data using Azure Data Lake Analytics (ADLA) U-SQL job. Then use [Pyomo](http://www.pyomo.org/) to convert the input into standard optimization problem formats, .nl or .mps.
- **Parallel optimization using Azure Batch**: Create inventory management policy by solving inventory optimization problems using BONMIN in Docker containers. We create a task for each data partition, e.g. store and product combination, and all the tasks are executed in parallell in an Azure Batch virtual machine pool.
- **Result post-processing**: ADLA U-SQL jobs use optimization problem solution, current time and inventory levels to place new orders. Both intermediate and final results are saved on Azure Data Lake Store
- **Orchestration and schedule**: A **Main** Azure Web Job is scheduled to run once every hour. This web job invokes the other web jobs that are executed according to the schedule of each inventory policy in an excel configuration file.
- **Visualize**: A PowerBI Dashboard is used to visualize inventory policy performance and inventory level.

## Solution Dashboard
Below is a snapshot of the Power BI dashboard that visualizes the results and relevant information of this inventory optimization solution.

![Power BI Dashboard Snapshot](https://github.com/Azure/cortana-intelligence-inventory-optimization/blob/master/Manual%20Deployment%20Guide/Figures/PowerBI_Dashboard.png)

The dashboard contains four parts:

1. **Inventory Management Policies**: shows the inventory optimization policies that have been created,
with Active Flag indicating which policy is active and Sim as a baseline policy to be compared with the
active policies.

2. **Performance Evaluation**: presents the evaluation metrics of inventory optimization, including Normalized
Revenue (NR), Total Revenue (TR), Number of Stockout Events (NSE), and Turnover Ratio (TOR).

3. **Inventory Status**: shows the inventory level aggregated over all the products of a specific set of stores
at the end of each day.   

4. **Store Information**: shows the information of the stores which have been simulated in this solution.

## Getting Started
This Solution How-To Guide contains materials to help both technical and business audiences understand our inventory optimization solution built on [Cortana Intelligence](https://www.microsoft.com/en-us/server-cloud/cortana-intelligence-suite/Overview.aspx).

### Bussiness Audiences
In this repository you will find a **[Solution Overview for Business Audiences](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Solution%20Overview%20for%20Business%20Audiences)** folder. This folder contains a walking deck with in-depth explanation of the solution for business audiences

For more information on how to tailor Cortana Intelligence to your needs, [connect with one of our partners](http://aka.ms/CISFindPartner).

### Technical Audiences
See the **[Manual Deployment Guide](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Manual%20Deployment%20Guide)** folder for a full set of instructions on how to deploy the end-to-end pipeline, including a step-by-step walkthrough and all the scripts that you’ll need to deploy this solution. **For technical problems or questions about deployment, please post in the issues tab of the repository.**

## Deep dive and customization
For more technical details of this solution and instructions on how to customize it, please see the **[Technical Guide](https://github.com/Azure/cortana-intelligence-inventory-optimization/tree/master/Technical%20Guide.pdf)**.

# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
