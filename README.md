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
- Create S3 bucket and respective folders. Upload the sample health data into s3 folders
- Create secret scope in Databricks, I followed the Databricks CLI way, below are the two resources used to create secret scope
  https://docs.databricks.com/aws/en/security/secrets/?language=Secrets%C2%A0utility%C2%A0%28dbutils.secrets%29
  https://www.youtube.com/watch?v=N3iLq79NfvY
  
  
