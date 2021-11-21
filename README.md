# 2021Z-SDBD-ProblemPlanProblems

A repository for a team project with Big Data.

## The members of the ProblemPlanProblems team

1. Przemysław Chojecki
2. Paweł Morgen
3. Paulina Przybyłek

## Project subject
Design and implementation of a data storage tool on press articles and analysis of their headlines.

### The goal of the project
The project will focus on performance and the solution will be designed with expansiveness in mind. Implemented solution will be highly scalable and will be able to process a high volume of data.

### Technology stack
The project will be the flow for data from [News API](https://newsapi.org/).  The data will be preprocessed by Apache NiFi. Preprocessed data it will be stored in Apache HBase. When the appropriate amount of data will be collected, the data will be processed (analiza wsadowa?) by Apache Spark, and the results will be stored in Apache Hive.
