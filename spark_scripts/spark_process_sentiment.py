"""ODPALAĆ Z OPCJĄ --packages=<bla bla bla>
JEST W PLIKU spark-submit-nlp.sh"""
from pyspark.sql import SparkSession 
from sparknlp.base import *
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline
import sparknlp

spark = SparkSession.builder \
	.appName("Process articles")\
	.master("local[2]").enableHiveSupport().getOrCreate()

articles = spark.sql("SELECT * FROM default.articles_test")
print('--- articles ---')
print(articles.printSchema())
print(articles.show())

# Sentiment
pipeline = PretrainedPipeline('analyze_sentiment', lang='en')
raw_annotations = pipeline.fullAnnotate(articles.withColumn('text', articles['article_title']), column='text')\
                  .withColumnRenamed('sentiment', 'raw_sentiment')
annotations = raw_annotations.withColumn('sentiment', raw_annotations['raw_sentiment']['result'][0])\
                             .withColumn('sentiment_confidence', raw_annotations['raw_sentiment']['metadata'][0]['confidence'])\
                             .drop('text', 'raw_sentiment', 'document', 'sentence', 'token', 'checked')
annotations_added = annotations.withColumn('confident_positive_sentiment', (annotations['sentiment'] == 'positive') & (annotations['sentiment_confidence'] > 0.5))\
                               .withColumn('confident_negative_sentiment', (annotations['sentiment'] == 'negative') & (annotations['sentiment_confidence'] > 0.5)) 
annotations_added.createOrReplaceTempView('articles_with_sentiment')

confident_sentiment = spark.sql('SELECT article_title, sentiment, sentiment_confidence, confident_positive_sentiment, confident_negative_sentiment FROM articles_with_sentiment ORDER BY sentiment_confidence DESC')
print(confident_sentiment.show(truncate=False))

# Publisher's summary
publishers_query = """ 
SELECT twitter_publisher_name,
    COUNT(*) AS total_published_articles,
    SUM(CAST(article_is_opinion AS INT)) AS total_published_opinions,
    ROUND(SUM(CAST(article_is_opinion AS INT)) / COUNT(*), 3) AS published_opinions_fraction,
    MAX(publisher_followers_count) as publisher_followers,
    SUM(article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM default.articles_test
WHERE twitter_publisher_name IS NOT NULL
GROUP BY twitter_publisher_name
ORDER BY total_published_articles DESC"""
publishers = spark.sql(publishers_query)
print('--- publishers ---')
print(publishers.printSchema())
print(publishers.show())


# topics summary
topics_query = """ 
SELECT article_topic,
    COUNT(*) AS total_published_articles,
    SUM(CAST(article_is_opinion AS INT)) AS total_published_opinions,
    ROUND(SUM(CAST(article_is_opinion AS INT)) / COUNT(*), 3) AS published_opinions_fraction,
    SUM(article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM default.articles_test
GROUP BY article_topic
ORDER BY total_published_articles DESC
""" 
topics = spark.sql(topics_query)

print('--- topics ---')
print(topics.printSchema())
print(topics.show())

