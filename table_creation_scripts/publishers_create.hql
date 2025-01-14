CREATE TABLE publishers (id STRING,
                         article_id STRING,
                         twitter_id BIGINT, 
                         twitter_account STRING, 
                         publisher_name STRING, 
                         location STRING, 
                         followers_count INT, 
                         list_count INT, 
                         number_of_tweets INT)
STORED AS AVRO;
