# Big Data-Apache-Project

A repository for a team project with Big Data.

## The members of the "ProblemPlanProblems" team

1. Przemysław Chojecki
2. Paweł Morgen
3. Paulina Przybyłek

## Project subject

Design and implementation of a data storage tool on press articles and analysis of their headlines.

### The goal of the project

The project will focus on performance and the solution will be designed with expansiveness in mind. Implemented solution will be highly scalable and will be able to process a high volume of data.

### Technology stack

The project will be the flow of data from [Free News API](https://free-docs.newscatcherapi.com/) and [Twitter API](https://developer.twitter.com/en/docs/twitter-api). The data will be acquired and preprocessed by Apache NiFi (including fusion of APIs). Raw and preprocessed data will be stored in HDFS. When the appropriate amount of data will be collected, the data will be batch processed by Apache Spark, and the results will be stored in Apache HBase.

### Business plan

The project will store data about articles such as the title, summary, published_date, topic, twitter_account of the publisher (e.g. @nytimes) and data about the publisher's Twitter account such as localization, followers, number of followers. Also, the number of tweets about the article 24 hours after publishing will be stored.

We will compare the sentiment of a summary with the amount of the tweets and/or topic and/or location of a publisher and/or number of Twitter followers.

This could provide meaningful information for authors about their audience and their's audience's preferences.
