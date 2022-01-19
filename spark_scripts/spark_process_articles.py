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

articles = spark.sql("SELECT * FROM default.articles_test")
#print('--- articles ---')
#print(articles.printSchema())
#print(articles.show())

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
#print(confident_sentiment.show(truncate=False))

# Publisher's summary
publishers_query = """ 
SELECT twitter_publisher_name,
    twitter_publisher_id AS row_key,
    COUNT(*) AS total_published_articles,
    SUM(CAST(article_is_opinion AS INT)) AS total_published_opinions,
    ROUND(SUM(CAST(article_is_opinion AS INT)) / COUNT(*), 3) AS published_opinions_fraction,
    ROUND(SUM(CAST(confident_positive_sentiment AS INT)) / COUNT(*), 3) AS articles_with_positive_sentiment_fraction,
    ROUND(SUM(CAST(confident_negative_sentiment AS INT)) / COUNT(*), 3) AS articles_with_negative_sentiment_fraction,
    MAX(publisher_followers_count) as publisher_followers,
    SUM(article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM articles_with_sentiment
WHERE twitter_publisher_name IS NOT NULL
GROUP BY twitter_publisher_id, twitter_publisher_name
ORDER BY total_published_articles DESC"""
publishers = spark.sql(publishers_query)
print('--- publishers ---')
#print(publishers.printSchema())
#print(publishers.show())

#column families: name, article_stats, twiter_stats
table = 'publishers_test'
write_to_file(publishers.select('row_key', 'twitter_publisher_name'), table, 'name')
write_to_file(publishers.select('row_key', 'total_published_articles', 'total_published_opinions', 'published_opinions_fraction', 'articles_with_positive_sentiment_fraction', 'articles_with_negative_sentiment_fraction'),
              table, 'article_stats')
write_to_file(publishers.select('row_key', 'publisher_followers', 'tweets_mentioning_articles_sum'),
              table, 'twitter_stats')

# topics summary
topics_query = """ 
SELECT article_topic,
    article_topic AS row_key,
    COUNT(*) AS total_published_articles,
    ROUND(SUM(CAST(confident_positive_sentiment AS INT)) / COUNT(*), 3) AS articles_with_positive_sentiment_fraction,
    ROUND(SUM(CAST(confident_negative_sentiment AS INT)) / COUNT(*), 3) AS articles_with_negative_sentiment_fraction,
    SUM(article_number_of_tweets) AS tweets_mentioning_articles_sum
FROM articles_with_sentiment
GROUP BY article_topic
ORDER BY total_published_articles DESC
""" 
topics = spark.sql(topics_query)

print('--- topics ---')
#print(topics.printSchema())
#print(topics.show())

# column families: article_stats, twitter_stats, name

table = 'topics_test'
write_to_file(topics.select('row_key', 'article_topic'), table, 'name')
write_to_file(topics.select('row_key', 'total_published_articles', 'articles_with_positive_sentiment_fraction', 'articles_with_negative_sentiment_fraction'),
              table, 'article_stats')
write_to_file(topics.select('row_key', 'tweets_mentioning_articles_sum'), table, 'twitter_stats')

