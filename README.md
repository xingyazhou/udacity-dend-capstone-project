# ETL Pipeline for Fire Department Calls and Temperature Data

### Data Engineering Capstone Project

#### Project Summary
The goal of this project: 
* Load San Francisco Fire department Calls-For-Service data set from Kaggle to AWS S3, 
* Create an ETL pipeline that extracts the data from S3, processes data using Spark, and loads the data back into S3 as set of dimensional tables. <br>


This will allow analytics team to find insights of the fire calls-for-service.e.g.
* What were the common types of fire calls?<br> 
* Which months within the year 2019 saw for the highest number of fire calls? <br>
* Which neighborhood in SF generated the most fire calls in 2019?<br>
* What is the longest reaction time when a fire call is trigged till the service is available?<br>
* What is the fastest reaction time when a fire call is trigged till the service is available?<br>
* How to improve in order to reduce the fire incident?<br>


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data<br>
* Step 2: Explore and Assess the Data<br>
* Step 3: Define the Data Model<br>
* Step 4: Run ETL to Model the Data<br>
* Step 5: Complete Project Write Up<br>

### Step 1: Scope the Project and Gather Data

#### Scope
In this project, we will aggregate fire call data by CallType to form first dimension table. Next we will aggregate city temperature data by year and month to form the second dimension table. The two datasets will be joined on year and month to form the fact table. The final database is optimized to query on fire call events to determine if temperature affects the number of fire incidents. Spark will be used to process the data.

 
#### Describe and Gather Data
**Data Source 1** <br>
San Francisco Fire Calls-For-Service <br>
https://www.kaggle.com/san-francisco/sf-fire-data-incidents-violations-and-more
    
**Content** <br>
Fire Calls-For-Service includes all fire units responses to calls. Each record includes the call number, incident number, address, unit identifier, call type, and disposition. All relevant time intervals are also included.

**Data Source 2**<br>

https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCity.csv
The temperature data comes from Kaggle. It is provided in csv format. Some relevant attributes include:

**Content** <br>
AverageTemperature = average temperature
City = city name
Country = country name
Latitude= latitude
Longitude = longitude


### Step 2: Explore and Assess the Data
#### Explore the Data 
Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.


#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project. <br>
Spark was chosen since it can easily handle large amounts of data.<br><br>

* Propose how often the data should be updated and why.
The data should be updated monthly. <br><br>


* Write a description of how you would approach the problem differently under the following scenarios:

  * The data was increased by 100x. <br>
Use AWS EMR + S3 if the data was increased by 100x<br><br>


  * The data populates dashboard that must be updated on a daily basis by 7am.<br>
Use Apache Airflow<br><br>


  * The database needed to be accessed by 100+ people.<br>
Store parquet files in AWS S3, give read access to users
