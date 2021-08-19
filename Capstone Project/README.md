# Capstone Project

This project consists of building an ETL pipeline that uses I94 immigration and temperature data to create a database optimized for analyzing immigration events. And the fact table will be used to answer whether the temperature of cities is decisive for the choice of destination by immigrants.

The project follows the follow steps:

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Step 1: Scope the Project and Gather Data

For this project we created two dimension tables and a fact table. One dimension table consists of I94 immigration data aggregated by destination city, the other dimension table is temperature data aggregated by city. Joining the two tables by city results in the fact table. The last step is the creation of a database to consult immigration events and check whether the temperature has an influence on the choice of destination by immigrants.

## Step 2: Explore and Assess the Data

Immigration data - filter data points that have valid i94port, rename columns with understandable names, extract number of the day from arrival date and convert dates to date format.

Temperature Data - drop data points where AvgTemperature is null, filter only US cities, add i94port in each entry and drop rows where i94port is null.

## Step 3: Define the Data Model

The purpose of this database is to aggregate immigration and temperature events using star schema, where the data is modeled in two dimensional tables linked to a fact table. Dimensional tables contain the characteristics of an event. The fact table stores the occurred facts and keys (i94port) for the corresponding characteristics in the dimensional tables.

## Step 4: Run Pipelines to Model the Data

* *Fact Table - I94 immigration data joined with the city temperature data (i94port):*

| **field**          | **type** | **description**                         |
|--------------------|----------|-----------------------------------------|
| i94port            | string   | i94 code (city code)                    |
| year               | int      | year                                    |
| month              | int      | month                                   |
| arrival_day       | date     |  arrival day in the USA                 |
| departure_date     | date     | departure date from the USA             |
| visa            | int      | visa code (1=Business, 2=Pleasure, 3=Strudent)      |
| mode            | int      | mode code (1=Air, 2=Sea, 3=Land, 9=Uninformed)      |
| city               | string   | destination city                               |
| temperature | numeric  | average temperature of destination city |


* *Dimension Table 1 - I94 immigration data Events:*

| **field** | **type** | **description**                    |
|-----------|----------|------------------------------------|
| i94port   | string   | i94 code (city code)               |
| year     | int      | year                               |
| month    | int      | month                              |
| origin_country    | int      | country code                       |
| origin_city    | int      | city code                       |
| arrdate   | date     | arrival date in the USA            |
| depdate   | date     | departure date from the USA        |
| mode            | int      | mode code (1=Air, 2=Sea, 3=Land, 9=Uninformed)      |
| visa            | int      | visa code (1=Business, 2=Pleasure, 3=Strudent)      |
| age   | int     | immigrant age        |
| gender   | string     | immigrant gender        |

* *Dimension Table 2 - Temperature data:*

| **field**           | **type** | **description**                         |
|---------------------|----------|-----------------------------------------|
| i94port             | string   | i94 code (city code)                    |
| temperature  | numeric  | average temperature of destination city |
| city                | string   | destination city                               |
| country             | string   | destination country                           |
| year            | int   | year                    |
| month           | int   | number of the month                   |
| day           | int   | number of the day                   |

## Step 5: Complete Project Write Up

#### Choice of tools

Apache Spark was used in the project due to its ability to work with large amounts of data and with different file formats, in addition to having spark.sql library has many tools for transforming data, such as performing joins and creating tables.

#### Data updates

Due to the nature of the data being monthly, the update is ideally carried out monthly.

#### Adapting the project to different scenarios

In this scenario Spark remains the tool to be used. To run this pipeline for a 100x dataset a real spark cluster must be used and distribute the calculation to several nodes.

For this case the ideal tool is Apache Airflow, with it you can reliably hijack and run ETL pipelines and report any problems along the way.

One option would be to put the data into S3 and create a pipeline with Airflow or AWS Step Functions to move the data from S3 to a scalable DataWarehouse on Amazon Redshift. If the database only receives queries and does not receive inserts or updates, the data can be periodically copied to a NoSQL database like Cassandra.