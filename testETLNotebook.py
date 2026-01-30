# Databricks notebook source
import os
# Install the required packages from the specified requirements file
dbutils.library.restartPython()
!pip install -r requirements.txt

# Import necessary libraries
from newsapi.newsapi_client import NewsApiClient
import pandas as pd
from newspaper import Article, Config
from nltk.corpus import stopwords
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import date, timedelta

def extract_transform_function():
    today = date.today()
    # Get today's date
    yesterday = today - timedelta(days = 1)
    # Get yesterday's date
    day_before_yesterday = today - timedelta(days = 2)
    # Get the day before yesterday's date

    # Initialize the News API client with an API key
    newsapi = NewsApiClient(api_key='236e1f294f584d60b147252e9ba7d4c3')

    # Get top headlines for the entertainment category in English, with a page size of 90
    top_headlines = newsapi.get_top_headlines(   
                                            category='entertainment',
                                            language='en',
                                            page_size = 90,
                                            page= 1)

    # Extract articles from the API response
    articles = top_headlines.get('articles',[])

    # Create a DataFrame from the articles, selecting specific columns
    init_df = pd.DataFrame(articles, columns = ['source','title','publishedAt','author','url'])

    # Extract the 'name' field from the 'source' dictionary in each row
    init_df['source'] = init_df['source'].apply(lambda x: x['name'] if pd.notna(x) and 'name' in x else None)

    # Convert 'publishedAt' to datetime format
    init_df['publishedAt'] = pd.to_datetime(init_df['publishedAt'])

    # Filter the DataFrame for articles published on the day before yesterday or yesterday
    filtered_df = init_df[(init_df['publishedAt'].dt.date == day_before_yesterday) | (init_df['publishedAt'].dt.date == yesterday)]
    # Rename the 'publishedAt' column to 'date_posted'
    filtered_df.rename(columns={'publishedAt': 'date_posted'}, inplace=True)

    # Make a copy of the filtered DataFrame
    df = filtered_df.copy()

    # Function to retrieve the full content of an article given its URL
    def full_content(url):
        print("url....", url)
        # Set up the user agent for the browser configuration
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
        config = Config()
        config.browser_user_agent = user_agent
        page = Article(url, config = config)

        try:
            # Download and parse the article
            page.download()
            page.parse()
            return page.text
        except Exception as e:
            print(f"Error retrieving content from {url}: {e}")
            return 'couldnt retrieve'

    # Apply the full_content function to each URL in the DataFrame
    df['content'] = df['url'].apply(full_content)
    # Replace newlines in the 'content' column with spaces
    df['content'] = df['content'].str.replace('\n', ' ')
    # Filter out rows where the content could not be retrieved
    df = df[df['content'] != 'couldnt retrieve']

    # Download the NLTK stopwords dataset and other required datasets
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')

    # Function to count words in a text excluding stopwords
    def count_words_without_stopwords(text):
        if isinstance(text, (str, bytes)):
            words = nltk.word_tokenize(str(text))
            stop_words = set(stopwords.words('english'))
            filtered_words = [word for word in words if word.lower() not in stop_words]
            return len(filtered_words)
        else:
            return 0

    # Apply the word count function to the 'content' column
    df['word_count'] = df['content'].apply(count_words_without_stopwords)

    # Download the VADER sentiment analysis lexicon
    nltk.download('vader_lexicon')

    # Initialize the SentimentIntensityAnalyzer
    sid = SentimentIntensityAnalyzer()

    # Function to get sentiment and compound score for a given text
    def get_sentiment(row):
        sentiment_scores = sid.polarity_scores(row)
        compound_score = sentiment_scores['compound']

        if compound_score >= 0.05:
            sentiment = 'Positive'
        elif compound_score <= -0.05:
            sentiment = 'Negative'
        else:
            sentiment = 'Neutral'

        return sentiment, compound_score

    # Apply the sentiment analysis function to the 'content' column
    df[['sentiment', 'compound_score']] = df['content'].astype(str).apply(lambda x: pd.Series(get_sentiment(x)))

    return df

# Call the extract_transform_function and store the result in a DataFrame
dataframe = extract_transform_function()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("CreateTableExample").getOrCreate()

# Define the schema explicitly if necessary
schema = StructType([
    StructField("source", StringType(), True),
    StructField("title", StringType(), True),
    StructField("date_posted", DateType(), True), 
    StructField("author", StringType(), True),
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
    StructField("word_count", IntegerType(), True),
    StructField("sentiment", StringType(), True),
    StructField("compound_score", DoubleType(), True)
])

spark_df = spark.createDataFrame(dataframe, schema=schema)
spark_df.write.mode('overwrite').saveAsTable('the_news.news_table')


# COMMAND ----------

# MAGIC  %sql
# MAGIC CREATE DATABASE IF NOT EXISTS the_news;
# MAGIC CREATE TABLE IF NOT EXISTS the_news.news_table (
# MAGIC source STRING,
# MAGIC title STRING,
# MAGIC date_posted DATE,
# MAGIC author STRING,
# MAGIC url STRING,
# MAGIC content STRING,
# MAGIC word_count INT,
# MAGIC sentiment STRING,
# MAGIC compound_score DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from the_news.news_table;