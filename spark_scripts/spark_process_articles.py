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

def get_file_name(table, column_family):
    sep = '-'
    ext = '.parquet'
    dir_path = "/user/chojeckip/project/hbase_buffer/"
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    list_status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(dir_path))
    result = [file.getPath().getName() for file in list_status]

    filename_root = f'{table}{sep}{column_family}'
    filename_suffix = 0
    filename = f'{filename_root}{sep}{filename_suffix}{ext}'
    while filename in result:
        filename_suffix += 1
        filename = f'{filename_root}{sep}{filename_suffix}{ext}'
    print(f'Saving to {filename} at {dir_path} in HDFS')
    return f'hdfs://localhost:8020{dir_path}{filename}'

def write_to_file(df, table, column_family):
    filename = get_file_name(table, column_family)
    df.write.parquet(filename)

articles = spark.sql("SELECT * FROM default.articles")
#print('--- articles ---')
#print(articles.printSchema())
#print(articles.show())

# Sentiment
pipeline = PretrainedPipeline('analyze_sentiment', lang='en')
raw_annotations = pipeline.fullAnnotate(articles.withColumn('text', articles['title']), column='text')\
                  .withColumnRenamed('sentiment', 'raw_sentiment')
annotations = raw_annotations.withColumn('sentiment', raw_annotations['raw_sentiment']['result'][0])\
                             .withColumn('sentiment_confidence', raw_annotations['raw_sentiment']['metadata'][0]['confidence'])\
                             .drop('text', 'raw_sentiment', 'document', 'sentence', 'token', 'checked')
annotations_added = annotations.withColumn('confident_positive_sentiment', (annotations['sentiment'] == 'positive') & (annotations['sentiment_confidence'] > 0.5))\
                               .withColumn('confident_negative_sentiment', (annotations['sentiment'] == 'negative') & (annotations['sentiment_confidence'] > 0.5)) 
annotations_added.createOrReplaceTempView('articles_with_sentiment')

#print('--- articles_tweets ---')
#print(annotations_added.printSchema())
#print(annotations_added.show())


join_tweets_query = """ 
SELECT articles_with_sentiment.id, 
    FIRST(articles_with_sentiment.published_date) AS published_date,
    FIRST(articles_with_sentiment.title) AS title,
    FIRST(articles_with_sentiment.author) AS author,
    FIRST(articles_with_sentiment.topic) AS topic,
    FIRST(articles_with_sentiment.is_opinion) AS is_opinion,
    FIRST(articles_with_sentiment.country) AS country,
    FIRST(articles_with_sentiment.confident_positive_sentiment) AS confident_positive_sentiment,
    FIRST(articles_with_sentiment.confident_negative_sentiment) AS confident_negative_sentiment,
    SUM(CASE WHEN tweets.id IS NULL THEN 0 ELSE 1 END) as article_number_of_tweets
FROM tweets RIGHT JOIN articles_with_sentiment ON tweets.article_id=articles_with_sentiment.id
GROUP BY articles_with_sentiment.id
"""
articles_tweets = spark.sql(join_tweets_query)
articles_tweets.createOrReplaceTempView('articles_tweets')

#print('--- articles_tweets ---')
#print(articles_tweets.printSchema())
#print(articles_tweets.show())

#confident_sentiment = spark.sql('SELECT article_title, sentiment, sentiment_confidence, confident_positive_sentiment, confident_negative_sentiment FROM articles_with_sentiment ORDER BY sentiment_confidence DESC')
#print(confident_sentiment.show(truncate=False))

# Publisher's summary
publishers_query = """ 
SELECT FIRST(publisher_name) AS publisher_name,
    publishers.twitter_id AS row_key,
    COUNT(*) AS total_published_articles,
    SUM(CAST(articles_tweets.is_opinion AS INT)) AS total_published_opinions,
    ROUND(SUM(CAST(articles_tweets.is_opinion AS INT)) / COUNT(*), 3) AS published_opinions_fraction,
    ROUND(SUM(CAST(articles_tweets.confident_positive_sentiment AS INT)) / COUNT(*), 3) AS articles_with_positive_sentiment_fraction,
    ROUND(SUM(CAST(articles_tweets.confident_negative_sentiment AS INT)) / COUNT(*), 3) AS articles_with_negative_sentiment_fraction,
    MAX(publishers.followers_count) as publisher_followers,
    SUM(articles_tweets.article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM publishers JOIN articles_tweets ON publishers.article_id=articles_tweets.id
WHERE publishers.publisher_name IS NOT NULL
GROUP BY publishers.twitter_id, publishers.publisher_name
ORDER BY total_published_articles DESC"""
publishers = spark.sql(publishers_query)

#print('--- publishers ---')
#print(publishers.printSchema())
#print(publishers.show())

#column families: name, article_stats, twiter_stats
table = 'publishers'
write_to_file(publishers.select('row_key', 'publisher_name'), table, 'name')
write_to_file(publishers.select('row_key', 'total_published_articles', 'total_published_opinions', 'published_opinions_fraction', 'articles_with_positive_sentiment_fraction', 'articles_with_negative_sentiment_fraction'),
              table, 'article_stats')
write_to_file(publishers.select('row_key', 'publisher_followers', 'tweets_mentioning_articles_sum'),
              table, 'twitter_stats')

# topics summary
topics_query = """ 
SELECT topic,
    topic AS row_key,
    COUNT(*) AS total_published_articles,
    ROUND(SUM(CAST(confident_positive_sentiment AS INT)) / COUNT(*), 3) AS articles_with_positive_sentiment_fraction,
    ROUND(SUM(CAST(confident_negative_sentiment AS INT)) / COUNT(*), 3) AS articles_with_negative_sentiment_fraction,
    SUM(article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM articles_tweets
GROUP BY topic
ORDER BY total_published_articles DESC
""" 
topics = spark.sql(topics_query)

#print('--- topics ---')
#print(topics.printSchema())
#print(topics.show())

# column families: article_stats, twitter_stats, name

table = 'topics'
write_to_file(topics.select('row_key', 'topic'), table, 'name')
write_to_file(topics.select('row_key', 'total_published_articles', 'articles_with_positive_sentiment_fraction', 'articles_with_negative_sentiment_fraction'),
              table, 'article_stats')
write_to_file(topics.select('row_key', 'tweets_mentioning_articles_sum'), table, 'twitter_stats')
