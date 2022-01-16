LOAD DATA INPATH 'hdfs://localhost:8020/user/morgenp/articles_test_portion.avro' OVERWRITE 
INTO TABLE articles_test;
