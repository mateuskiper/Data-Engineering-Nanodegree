# Project: Data Modeling with Apache Cassandra

To complete the project it was necessary to model part of the ETL pipeline that transfers data from a set of CSV files into a directory to create a simplified CSV file for modeling and inserting data into Apache Cassandra tables.

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Data

For this project, the dataset is in CSV files partitioned by date.