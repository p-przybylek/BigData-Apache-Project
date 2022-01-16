from pyspark.sql import SparkSession 
#from sparknlp.base import *
#from sparknlp.annotator import *
#from sparknlp.pretrained import PretrainedPipeline
#import sparknlp
#from pyspark.ml.pipeline import PipelineModel

spark = SparkSession.builder \
	.appName("Process articles")\
	.master("local[2]").enableHiveSupport().getOrCreate()

articles = spark.sql("SELECT * FROM default.articles_test")
print('--- articles ---')
print(articles.printSchema())
print(articles.show())



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

