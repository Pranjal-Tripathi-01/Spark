# Kafka-MongoDB-Spark

An ETL pipeline integrating MongoDb, Kafka and Spark.


## Project Description

1. CSV files from local is sent to Kafka topic using producer.py
2. Data from Kafka topic is consumed written to MongoDB Atlas using consumer.py
3. Data from MongoDB Atlas is loaded into Spark dataframe and some analyics are performed.  

## Resources:
1. MongoDB Atlas for MongoDB
2. Confluent Cloud for Kafka

## Images

<p align="center">
  <img src="https://github.com/Pranjal-Tripathi-01/Spark/blob/main/Screenshot%20from%202023-05-07%2013-36-18.png" title="Kafka">
  <img src="https://github.com/Pranjal-Tripathi-01/Spark/blob/main/Screenshot%20from%202023-05-07%2013-22-32.png"  title="Kafka Consumer">
  <img src="https://github.com/Pranjal-Tripathi-01/Kafka/blob/main/Screenshot%20from%202023-04-19%2021-42-44.png"  title="Cassandra">  
</p>
