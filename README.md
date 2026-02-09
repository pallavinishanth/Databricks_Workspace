# Databricks_Workspace

This workspace is created for some Databricks DataEngineering and GenAI projects

DataEngineering projects:
- News ETL pipeline: 'testETLNotebook.py' is sample notebook that pulls data from NewsAPI and perform some ETL operations on it. A job is scheduled to run the notebook on preferred days and times. Created personal compute cluster for compute. Added Triggers to notify if the job is success or has some issues.
-  

GenAI projects:

**Health RAG chatbot:** 

High level architecture of the project

<img width="1536" height="1024" alt="Healthchatbot_architecture" src="https://github.com/user-attachments/assets/9312dab5-111a-4ca0-a0a7-db40b24b5c59" />

- First step is to set up Databricks workspace on AWS, followed steps in this link 
https://aws.amazon.com/blogs/awsmarketplace/start-using-databricks-data-intelligence-platform-with-aws-marketplace/

Note: Once we set up Databricks workspace on AWS, AWS automatically creates a NAT gateway which costs $0.045/hr just to exist. Even if your Databricks environment has zero clusters turned on, zero workloads running, its mere presence will result in $0.045 charged every hour. We can delete NAT Gateway when we are not using workspace for longer time, but need to check route tables are correct, DNS hasn't cached failures. If not recereated properly our cluster setup throws BOOTSTRAP_TIMEOUT / INSTANCE_INITIALIZING error. You will be charged for the compute as well, but while creating compute in databricks we can set autoterminate option.

- Create S3 bucket and respective folders. Upload the sample health data into s3 folders
- Create secret scope in Databricks, I followed the Databricks CLI way, below are the two resources used to create secret scope
  https://docs.databricks.com/aws/en/security/secrets/?language=Secrets%C2%A0utility%C2%A0%28dbutils.secrets%29
  
  https://www.youtube.com/watch?v=N3iLq79NfvY

- A job is created to ingest sample csv files from s3 to databricks bronze layer. 'ingest_healthdata_to_bronze' file is the ingestion code. A files log table is created to record file signatures (source_file + file_size + file_mod_time) if the job reruns, it skips files with signatures already marked SUCCESS. We can also enable archiving for “strongest” protection.

- Snapshot data profiling is created to schedule data quality monitoring. 'data_quality_monitor' notebook is created to schedule monitoring job. Data profiling provides summary statistics for a table, computing profiling metrics over time so you can easily view historical trends. It is useful for in-depth monitoring of all key metrics for select tables. You can also use it to track the performance of machine learning models and model-serving endpoints by profiling inference tables that contain model inputs and predictions.

- Data cleansing is performed on all the tables. I used overwrite method for this project because data is so small, in production incremental method is used so that only the new/changed data gets cleansed and updates the table. Performed removing duplicates, normalized date formats etc
  
  
