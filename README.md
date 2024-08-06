# OVERVIEW
In this project, an end-to-end data pipeline for multi-stage data processing by utilizing different technologies is constructed. Here, the MOT (Ministry of Transport) dataset is cleaned, transformed and the results are shown using a dashboard. 

## 1. PROJECT DESCRIPTION
This project uses MOT dataset provided by the Department of Transport, UK. MOT tests are conducted for the purpose of ensuring that vehicles over a prescribed age are inspected at least once a year to view that they follow key roadworthiness and environmental requirements. It consists of the data collected during the annual vehicle inspection. Upon completion, test certificates are issued. 

The datasets consist of MOT testing data results.  The data will be used to answer the following questions: 
- Which brand has the most failure test?
- Which model of the specific brand has the most failure test?
- Reason for rejection during the test.
  
This will be useful to automakers for gaining insights about the improvement needed in the failed part and for providing recommendations to buyers.

The key goals of the project are:
1. Create a data pipeline that will help to organize data processing
2. Build an analytical dashboard to help users discover trends and insights.


## 2.	DATASET
The data set can be found here: Anonymized MOT tests and results - data.gov.uk 
2.1	Vehicle test result
This contains information about the time, place, and final outcome of the MOT test in addition to information about the vehicle tested, it includes the following columns:
| Column Name | Description |
| --- | --- |
|Test ID	| Unique Identifier for a test |
| Vehicle ID | Unique Identifier for a vehicle |
| Test Date	| Date of Test |
| Test Class ID	| Class of Vehicle Tested |
| Test Type	| Type of MOT Test |
| Test Result |	Test Outcome |
| Test Mileage	| Mileage recorded at point of test |
| Postcode Area |	Test Location |
| Make	| Vehicle Make |
| Model	| Vehicle Model |
| Colour	| Vehicle Colour |
| Fuel Type |	Vehicle Fuel Type |
| Cylinder Capacity	| Vehicle Cylinder Capacity |
| First use Date	| Vehicle Date of First Use |

2.2 Vehicle test item

This contains details of individual M.O.T. test failure items, it includes the following columns:
| Column Name	| Description |
| --- | --- |
| Test ID	| Unique Identifier for a test |
| RfR ID	| Reason for Rejection ID |
| RfR Type | Reason for Rejection Type |
| Location ID	| Failure Location ID |
| D Mark	| Dangerous Item Marker |


## 3. TECHNOLOGIES USED
### 3.1 Docker
It is an open platform for developing, testing and deploying applications quickly. It delivers software in packages known as containers. The docker container is an isolated environment which includes applications and all their dependencies. Docker is used in this project, to encapsulate package dependencies the code may have, which allows the code to run any data. Docker is used in this project to deploy Apache Airflow.
### 3.2 Google Cloud Platform (GCP)
It is a suite of cloud computing services that runs on the same infrastructure that Google uses internally for its end-user products, such as Google Search, Gmail, Google Drive, and YouTube. In this project we use the following GCP products:
- Google Cloud Storage: A data lake provides a scalable and secure platform to ingest and store large amounts of data ignoring size limits is collected and stored in data lakes. Google Cloud Storage can store any amount of data and retrieve it as often as required. It is chosen due to its strong consistency, performance, flexible processing
- BigQuery: a completely serverless and cost-effective enterprise data warehouse. It uses SQL and focuses on analyzing data to find meaningful insights.
### 3.3 Terraform
It is an open-source infrastructure as a code (IaC) tool developed by HashiCorp. It is a cloud infrastructure provisioning and managing tool written in the Go language. This project uses terraform to build and manage the GCP infrastructure.
### 3.4 Apache Airflow
It is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. It is used as a tool for the orchestration of pipelines/workflows due to its dynamic, extensible, and flexible behavior. It allows tasks to be defined in a Directed Acyclic Graph (DAG) with dependencies allowing the tasks to be optimized. In case of failure in any part of the pipeline, it allows users to check the steps. This project uses Airflow to build an automated data pipeline.
### 3.5 Google Data Studio
Also known as Looker Studio, it is a free reporting and visualization tool. Widgets include pie charts, heat graphs, and many more. It can also pull data from multiple sources. It allows us to connect, visualize and share reports.

## 4. ARCHITECTURE
The data pipeline includes the following steps:
- M.O.T. dataset files (CSV) are downloaded, cleaned, then converted to parquet files
- Parquet data set files are uploaded to the data lake (Google Cloud Storage)
- Data Tables for the 2 data sets are created in the Data Warehouse (Google BigQuery) and are populated with the data from the parquet files in the Data Lake
- Partitioning and Clustering is done on the data in the Data Warehouse to be ready for analysis
After the data is transformed and ready, it will be loaded in the Dashboard created in Google Data Studio.
![image](https://github.com/user-attachments/assets/7cef17cf-21ac-49ad-940d-ca60dd4f01f3)


## 5. PREREQUISITES
The following prerequisites must be satisfied before creating the Docker stack in order to be able to set it up:
- Docker: Install Docker Desktop & verify that Docker-Compose is installed using these instructions (https://docs.docker.com/compose/install/)
- Terraform: Install Terraform using these instructions (https://developer.hashicorp.com/terraform/downloads)
- Google Cloud Platform: Create a GCP account, create a project with a unique ID, and export the Service account JSON using the instruction.
  1. Create an account for GCP. Set up a new project and write down the Project ID.
  2. From the GCP dashboard, select the project from the drop-down menu.
     ![image](https://github.com/user-attachments/assets/06774f34-1829-423d-a124-259d21e9bfc7)

  4. Set up a service account for the project and download JSON authentication key files.
     - IAM & Admin>Service accounts>Create service accounts
       ![image](https://github.com/user-attachments/assets/40c12035-7be5-4c28-b16a-6f3e242397b1)

     - Provide a service account name. Leave all other fields with default values. Click on Create and continue.
       ![image](https://github.com/user-attachments/assets/cb512e4b-9868-450e-a7ee-ca84f68424dc)

     - Grant the Viewer role (Basic>Viewer) to the service account. Click on Continue. There is no need to grant users access to this service account at the moment. Click on Done.
       ![image](https://github.com/user-attachments/assets/23336528-c39b-474c-9666-cb7715cfbb4d)

  5.  Service account is created, click on the 3 dots below Actions and select Manage keys.
     ![image](https://github.com/user-attachments/assets/9e0a6213-f8db-488b-8d35-d7d03252983e)

  7.  Add key>Create new key. Select JSON and click Create. Save them to a folder. After adding the key, we have an active key as seen below:
     ![image](https://github.com/user-attachments/assets/c0122166-061c-4cf0-b1e9-c0673fcd6c6f)

  9.  Assign the following IAM Roles to the service account: Storage Admin, Storage Object Admin, BigQuery Admin and Viewer. (Viewer already present as role)
      - Select the previously created Service account and edit permissions by clicking on the pencil shaped icon on the left.
      - Click Add Another Role. Add the following roles: Storage Admin, Storage Object Admin, BigQuery Admin and click save.
        ![image](https://github.com/user-attachments/assets/201cf43a-2ace-4f42-b64f-3a319d7322b2)

      - Now, we can see that the new roles have been added to the service account.
        ![image](https://github.com/user-attachments/assets/121083af-6a4b-4da3-a5f4-4f63af99067d)

  10.  Enable APIs for the project:
      - https://console.cloud.google.com/apis/library/iam.googleapis.com
       ![image](https://github.com/user-attachments/assets/1aacc1d5-8a31-4da0-a74e-5da4f4b47e08)
      - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
       ![image](https://github.com/user-attachments/assets/399586e4-b1c6-4b29-99bb-fd816ccf483f)


## 6. SETUP
   
6.1 Folder Structure

The first step is preparing the working directory using the following steps:
 1. Create a folder on your machine in any location (going forward we'll refer to that directory as CREATED_DIR)
 2. Navigate to CREATED_DIR and create a folder named "google" inside of it and paste the service account JSON file that we exported from GCP (Please refer to prerequisites)
 3. Extract the content of the project zip file to CREATED_DIR (make sure that CREATED_DIR is the root to all the files and subfolders in the zip file and that they're not extracted to a subfolder), CREATED_DIR should look similar to this:
   ![image](https://github.com/user-attachments/assets/9d423c2c-3c4a-481b-b1c2-6b268beaeb3c)

6.2 Accommodating for unique GCP IDs
This step updates both Terraform configuration and Docker-Compose with the newly created GCP project id and service account JSON
 1. Open CREATED_DIR/terraform/variables.tf with any text (preferably code) editor and update 'project' variable’s default value with the id of the project you created on GCP.
 2. Open CREATED_DIR/docker-compose.yaml with any text (preferably code) editor and update the following:
    1. Update the "GCP_PROJECT_ID" env variable with the id of your created GCP project
    2. Add the id of your created GCP project to the env variable "GCP_GCS_BUCKET" after the string "ue-de-final-bucket_"
    3. Update both "GOOGLE_APPLICATION_CREDENTIALS" & "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT" with the name of the JSON file you downloaded and copied to CREATED_DIR/google

6.3 Terraform GCP credentials
In order for Terraform to be authenticated on GCP to perform infrastructure management, we need to make sure that the service account JSON file is properly mapped in our system’s environment variables. In order to do this, run on of the following commands (based on your OS) in command line/terminal (make sure to change {PATH_TO_CREATED_DIR} to the actual full path of CREATED_DIR):
  - set GOOGLE_APPLICATION_CREDENTIALS= {PATH_TO_CREATED_DIR}/google/{JSON_FILE}
                                       OR
  - export GOOGLE_APPLICATION_CREDENTIALS= {PATH_TO_CREATED_DIR}/google/{JSON_FILE}

6.4 Creating and running the Docker stack
In this step, we spin up the containers and run our Airflow environment.
 1. Open the command line/terminal, then navigate to CREATED_DIR
 2. Enter the following command:
docker-compose -p "folder_name" up --build
 3. This will take a while to build, please be patient. When the build is completed successfully, the health check of the airflow-webserver will be shown (similar to the one below).
    ![image](https://github.com/user-attachments/assets/0fa7c820-7c72-45a5-aaad-3b3f08027825)

 5. From docker desktop, we can see that “folder_name” is created. It contains the containers shown below:
    ![image](https://github.com/user-attachments/assets/d1355cfb-056e-43f3-86ca-d06b6ef52f64)


## 7. TESTING
   
### 7.1 Infrastructure
Now that everything is set up, we need to create the infrastructure (Data Lake/ Data Warehouse) on GCP using terraform. To do this, we need to open a new command line/terminal, navigate to CREATED_DIR then run the following commands:
 1. terraform init:
    If the command is run successfully, the output is shown as below:
    ![image](https://github.com/user-attachments/assets/16a35e93-d282-4559-92b0-00b0acdf8ad2)

 3. terraform plan:
    If the command is run successfully, the output is shown as below:
    ![image](https://github.com/user-attachments/assets/5408c72b-c2a9-4182-a7d6-061b8c29fc65)

 5. terraform apply:
    For the last command, make sure to enter “yes” when prompted and verify that 2 resources are created successfully. If the command is run successfully, the output is shown as below.
    ![image](https://github.com/user-attachments/assets/97e52531-7434-4ed8-b784-6272a61313c2)

Once this is done, we can see that data warehouse (BigQuery) and data lake (Cloud Storage) resources are created in the GCP platform (as shown in the figures below.

![image](https://github.com/user-attachments/assets/3189c775-55a3-446b-a8f6-73d6c9e648bf)

![image](https://github.com/user-attachments/assets/cca366c1-a087-4a9a-96d1-c53321497f2c)

### 7.2 Running the Pipeline
Finally, we need to open the airflow and run our pipeline.
 1. Open your browser of choice, then navigate to http://localhost:8080
 2. Log in to airflow using Username: airflow / Password: airflow
    ![image](https://github.com/user-attachments/assets/ace493bc-a91e-479e-b407-9ab00dc9ac0f)

 3. From the DAGs list, switch the toggle next to “mot_de_group16”. And click on the DAG name “mot_de_group16”.
    ![image](https://github.com/user-attachments/assets/1d597e0f-5b1f-4a77-9bf4-ed6cad7411f5)
    ![image](https://github.com/user-attachments/assets/3483319b-3087-431c-b254-edb1308439ec)

 4. Now you can monitor the pipeline either using the Grid or Graph options. If the pipeline is running successfully, it 
 will show success. And the border of each module of the pipeline will be in color.
    ![image](https://github.com/user-attachments/assets/31852bac-140b-43f1-a31f-0ce92eba713b)

   - Click on the grid option. It will open the grid similar as shown below.
     ![image](https://github.com/user-attachments/assets/0e9e28b7-ec18-4b0b-809c-6aff65e5934a)
   - Click on the graph option. The layout of the graph can be updated. We use Top to down graph layout as shown below:
     ![image](https://github.com/user-attachments/assets/f4cfcf51-927a-4655-8e8a-886aa429fa7b)


### 7.3 DAG Breakdown
In this section, you’ll find a brief description of each of the graph steps (green steps are dummy operators, so no explanation is required). Please note that these steps are duplicated for the 2 tables “result” and “item”, so the description below applies to both.
 1. download_*: this is a python operator that calls a cutom function which uses the wget command to download the file from the M.O.T. link
 2. extract_*: this is a python operator that calls a custom function which extracts the zip file downloaded in the previous step, then renames all the files in a serial order to be accessible throughout the pipeline
 3.clean_*: this is a python operator that calls a custom function which removes the zip file and the original unnamed extracted files
 4. format_to_parquet_task_*: this is a python operator which calls a custom function which loads the csv file into a Pandas data frame, cleans the data in the data frame *steps are explained in the code* then exports the data frame as a parquet file
 6. *_csv_cleanup: this is a bash operator that deletes the csv file that was converted to parquet
 7. *_parquet_to_gcs_task: this is a python operator that uploads the parquet file to “raw” folder in the Cloud Storage Bucket created and configured in the previous steps
 8. *_parquet_cleanup: this is a bash operator that deletes the parquet file that was uploaded to the Cloud Storage Bucket
 9. bigquery_delete_*: this is a BigQueryDelete Operator which makes sure that the table we’re trying to create doesn’t exist in the Data Set (since this pipeline is used to load the data once and not append to existing data)
 10. bigquery_create_*: this is a BigQueryCreateExternalTable Operator which creates the tables using the uploaded the parquet files in the Cloud Storage Bucket
 11. bigquery_create_partitioned_*: this is a BigQueryExecuteQuery Operator which runs our SQL Queries (explained below) to created the partitioned and clustered tables

### 7.4 Verification
After the pipeline is completed successfully, the process can be verified by following the below steps:
a. Open your browser of choice and navigate to your google cloud project.
b. Make sure that your project is selected then from the menu list click on “Cloud Storage”, The created bucket should appear and inside of it is a folder “raw” containing the Parquet files as shown below:
![image](https://github.com/user-attachments/assets/b7e570ea-3744-4df8-8a87-b154dbc56c70)

![image](https://github.com/user-attachments/assets/46c8b5f2-80ce-406a-85d0-645d4b4727dd)

c. From the main menu, navigate to “Big Query”, The created tables should appear under the project as shown below:
![image](https://github.com/user-attachments/assets/2bef9575-66c5-49d3-88e3-b225ee3d3491)


### 7.5 Partitioning and Clustering
After loading the data in Cloud Storage and Big Query, we will structure our data to match common data access pattern. This will make our queries run faster while spending less. For this purpose, we are going to do partitioning and clustering. 

Here, we are going to do partitioning by using the date data type as BigQuery supports only this date type for partitioning. As a result, we created 2 partitioned table i.e. test_result_partitioned and test_item_partitioned. Clustering is done using “make” of the vehicles. So, the 2 partitioned table is clustered by make.

The following queries are used to do the partitioning and clustering: -- result table, partitioned by test_date with Month interval and clustered by Make CREATE OR REPLACE TABLE `mot_data.test_result_partitioned` PARTITION BY TIMESTAMP_TRUNC(test_date, MONTH) CLUSTER BY make AS SELECT test_id, test_date, test_result, make, model FROM `mot_data.test_result` WHERE test_class_id = 4 AND test_type = 'NT' AND (test_result='F' OR test_result='ABA') -- item table, partitioned by test_date with Month interval and clustered by Make CREATE OR REPLACE TABLE `mot_data.test_item_partitioned` PARTITION BY TIMESTAMP_TRUNC(test_date, MONTH) CLUSTER BY make AS SELECT rfr_id, r.test_result, f.rfr_type_code,r.test_date, r.make, r.model, f.dangerous_mark FROM `mot_data.test_failure` f JOIN `mot_data.test_result` r on f.test_id = r.test_id WHERE r.test_class_id = 4 AND r.test_type = 'NT' AND (r.test_result='F' OR r.test_result='ABA')

We can see that the tables are created as shown below:
![image](https://github.com/user-attachments/assets/171d110e-d747-46df-9852-f0c1c3c988ba)

The newly created 2 partitioned tables has the fields as shown below:
![image](https://github.com/user-attachments/assets/3b2b88b1-5747-4f1e-851f-7c154964525a)
![image](https://github.com/user-attachments/assets/aa3b78ba-a20c-4e05-965b-8bdc5fb5d67f)



## 8. RESULTS
### 8.1 Dashboard
The dashboard is created using the Google Data Studio (Looker Studio).
 - Open the Looker Studio and create a blank report then connect with BigQuery.
 - Choose the project and click on apply. Create tables by selecting the dimensions and metrics. Add the charts. Customize it according to our requirements by clicking the properties and data.
Here, we used 3 charts to represent our findings.
 - Column chart: It displays 15 make with the most failure results.
 - Pie chart: It displays the models of a particular make with the most failure result.
 - Doughnut chart: It displays the reasons of rejection of a particular make.
   
  | RfR Type |	Type Code |	Notes |
  | --- | --- | --- |
  |Fail	F |	A | test failure item. |
  | PRS	| P	| An item in a failing state at the point of test, repaired within one hour of the test and before the result was entered. |
  | Advisory |	A	 | An advisory item |
  | Minor defect	| M	| Minor defect |


The dashboard below shows the Mot test failure result of Ford. We can find the following from the chart:
 - Top 3 make with the most failure results are Ford, Vauxhall, Volkswagen.
 - Top 2 models of Ford with the most failure results are Fiesta and Focus.
 - Ford failed during the test for different reasons. 54.7 % due to the failure items, 40.1 % due to an advisory item (items that are wearing and not broken but deemed unfit to use) and remaining due to other reasons.
   ![image](https://github.com/user-attachments/assets/2d65ce6c-5999-41b4-b98d-03d48bc14385)
The dashboard below shows the MOT test failure results of Vauxhall.
![image](https://github.com/user-attachments/assets/7017f21e-5612-435b-86cf-e8be6a90178d)

 
***Note: For displaying the charts, we use 2020- 2021 MOT data***



## 8.2 Conclusion
As part of the project, we have explored and use different technologies for creating an end-to-end data pipeline to ingest M.O.T. datasets then perform some data cleaning and preprocessing. We use Terraform to manage our infrastructure i.e., the Google Cloud Platform (GCP) where the Cloud Storage (data lake) and BigQuery (data warehouse) are hosted. We also use Docker to host the airflow. Airflow builds, orchestrates and monitors the workflow of our data pipeline. For the editor, we use Visual Code Studio (VSCode) to create anCd manage our script files (.py,.tf,.yml). Lastly, for creating the dashboard, we use the Google Data Studio in order to display our finding and answer the questions.
