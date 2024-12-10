Qustion : 

Purpose of the Exercise
 
The purpose of the exercise is two-fold. Firstly, we want to validate technical skills using a JVM programming language and a batch or streaming framework that you are familiar with (Storm, Flink, Spark, etc). Secondly, we want to assess engineering practices.
 
Submission Requirements
 
·         Use Java, Kotlin or Python and provide testing evidence.
·         Code and documentation need to be uploaded to GitHub along with tests cases.
 
Data Source
 
The dataset can be downloaded from IMDB https://datasets.imdbws.com/. Please note that this dataset is just an example, the application should be expected to handle millions of records.
 
Problem Statement
 
Your task is to write a streaming application that can answer the following questions using the imdb data set.
 
1. Retrieve the top 10 movies with a minimum of 500 votes with the ranking determined by:
(numVotes/averageNumberOfVotes) * averageRating
 
2. For these 10 movies, list the persons who are most often credited and list the
different titles of the 10 movies.
 
The application should:
-          be runnable on a local machine
-          have documentation on how to execute
-          be reproducible







Solution :


# IMDB Streaming Application

Folder Structure

├── imdb-streaming-app/
    ├── src/
        ├── main.py
        ├── processor.py
        
    ├── tests/
        ├── test_processor.py
       
    ├── README.md
    ├── requirements.txt


## Overview
This application processes IMDB datasets to:
1. Retrieve the top 10 movies based on a ranking formula.
2. List persons credited for these top 10 movies.

## Prerequisites
- Python 3.x
- Apache Spark

## How to Run
1. Place IMDB `.tsv` files in the `data/` folder.
2. Run the application:
   ```bash
   python main.py
