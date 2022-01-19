CREATE TABLE articles (id STRING, 
                       published_date STRING, 
                       title STRING,  
                       author STRING, 
                       topic STRING, 
                       country STRING, 
                       language STRING, 
                       is_opinion BOOLEAN, 
                       querry STRING, 
                       summary STRING, 
                       number_of_tweets INT)
STORED AS AVRO;
