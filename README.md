# Databricks_Workspace

This workspace is created for some Databricks DataEngineering and GenAI projects

DataEngineering projects:
- News ETL pipeline: 'testETLNotebook.py' is sample notebook that pulls data from NewsAPI and perform some ETL operations on it. A job is scheduled to run the notebook on preferred days and times. Created personal compute cluster for compute. Added Triggers to notify if the job is success or has some issues.
-  

GenAI projects:

**Health RAG chatbot:** 

High level architecture of the project

<img width="1536" height="1024" alt="Chatbot_Architecture" src="https://github.com/user-attachments/assets/427b09e4-3365-41d9-bcbf-b7fe73c86f23" />

- First step is to set up Databricks workspace on AWS, followed steps in this link 
https://aws.amazon.com/blogs/awsmarketplace/start-using-databricks-data-intelligence-platform-with-aws-marketplace/

Note: Once we set up Databricks workspace on AWS, AWS automatically creates a NAT gateway which costs $0.045/hr just to exist. Even if your Databricks environment has zero clusters turned on, zero workloads running, its mere presence will result in $0.045 charged every hour. We can delete NAT Gateway when we are not using workspace for longer time, but need to check route tables are correct, DNS hasn't cached failures. If not recereated properly our cluster setup throws BOOTSTRAP_TIMEOUT / INSTANCE_INITIALIZING error. You will be charged for the compute as well, but while creating compute in databricks we can set autoterminate option.

- Create S3 bucket and respective folders. Upload the sample health data into s3 folders
- Create secret scope in Databricks, I followed the Databricks CLI way, below are the two resources used to create secret scope
  https://docs.databricks.com/aws/en/security/secrets/?language=Secrets%C2%A0utility%C2%A0%28dbutils.secrets%29
  
  https://www.youtube.com/watch?v=N3iLq79NfvY

- A job is created to ingest sample csv files from s3 to databricks bronze layer. 'ingest_healthdata_to_bronze' file is the ingestion code. A files log table is created to record file signatures (source_file + file_size + file_mod_time) if the job reruns, it skips files with signatures already marked SUCCESS. We can also enable archiving for “strongest” protection. (Currently I scheduled it every month)

- Snapshot data profiling is created to schedule data quality monitoring. 'data_quality_monitor' notebook is created to schedule monitoring job. Data profiling provides summary statistics for a table, computing profiling metrics over time so you can easily view historical trends. It is useful for in-depth monitoring of all key metrics for select tables. You can also use it to track the performance of machine learning models and model-serving endpoints by profiling inference tables that contain model inputs and predictions.

- Data cleansing is performed on all the tables. I used overwrite method for this project because data is so small, in production incremental method is used so that only the new/changed data gets cleansed and updates the table. Performed removing duplicates, normalized date formats, gender, body temperature etc.

- A new job or new task can be added to the workflow to automate the data cleansing process. We can add cleansing task dependent to previous ingestion tasks so that it automate the ingestion and data cleansing process.

- A dimension table 'dim_patient' is created and added to gold layer. The implementation is in 'created_dim_patient' natebook, this notebook is added as new task under workflows. This concludes setup up of all three layers. Here we have to create new column with all the data in the columns combined, because in later steps we use this column to create embeddings for the embedding model to use.

- A vector search endpoint is created manually, the creation steps are in 'vector_search_endpoint'. We have an option to create this in databricks UI as well.

- I decided to use databricks pre-configured embedding model 'databricks-bge-large-en' to create embeddings as it doesn't need to create any extra embedding model endpoint which is required for external models or some custom models to use. 'databricks-bge-large-en' is pay-per-token foundation model where we just query it.

- A vector index table 'patient_vector_table' is created under gold schema. To create this vector index we need to provide source table, vector search endpoint and the embedding model used to generate embeddings. After vecotr index is created a new column with embeddings appears in 'patient_vector_table'

- Now its time to create an app to test our RAG pipeline. I decided to create simple Streamlit chatbot app, to do this, we have option to create one under databricks app. This creates an app under databricks-app folder. The entry point for our app is app.py file, It’s the script Databricks runs to start your UI and wire it to platform services like Vector Search and Model Serving. Under 'App resources' make sure to add your vector search index and serving end point.
    Note: For this project I decided to use Databricks hosted serving endpoint 'databricks-meta-llama-3-1-405b-instruct', if you want to use some different llm's for your inference you can create custom serving endpoint.
  
- That's it, once we deploy this app we will be taken to the new website with our chatbot. Try it with some prompts and verify your results.

  "One last most important thing i want to mention here is about query_type parameter in method w.vector_search_indexes.query_index(). If you do NOT explicitly pass query_type, Databricks defaults to: ANN (Approximate Nearest Neighbor) which is great for conceptual questions and information retreivals but it doesn't work for exact match of patient_id lookups. There is another type which is "FULL_TEXT" useful for exact or near-exact string look up. In this project i used Hybrid meaning, when the query has patient id in it i am using FULL_TEXT if not ANN".
  
  
  
