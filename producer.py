import csv, json
import pandas as pd
from confluent_kafka import Producer


# Kafka topic and bootstrap server
api_key = 'EQT36REMCPAUFDZ3'
api_secret = 'EmAA2ZxHdJQ8YtnjhlwBl6TUzhKTIBWB2AheQorWrukBBt5NYRTZtm26YFf5endY'
schema_registry_api_key = 'YJZ4ZFTEL6FPXOBW'
schema_registry_api_secret = '3roetw5fZ1hKmxohm5/Ku+rAMPTBREc26QaQlJ82B6H0zxAukLoPOi1Jh43/G7be'
endpoint_schema_url = 'https://psrc-35wr2.us-central1.gcp.confluent.cloud'
bootstrap_servers = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
security_protocol = 'SASL_SSL'
ssl_mechanism = 'PLAIN'


# Kafka topics
kafka_topic1 = 'case'
kafka_topic2 = 'patient_info'
kafka_topic3 = 'policy'
kafka_topic4 = 'region'
kafka_topic5 = 'search_trend'
kafka_topic6 = 'seoul_floating'
kafka_topic7 = 'time'
kafka_topic8 = 'time_age'
kafka_topic9 = 'time_gender'
kafka_topic10 = 'time_province'
kafka_topic11 = 'weather'


# File locations
file_1= '/home/pranjal/Desktop/spark_assignment/dataset/Case.csv'
file_2= '/home/pranjal/Desktop/spark_assignment/dataset/PatientInfo.csv'
file_3= '/home/pranjal/Desktop/spark_assignment/dataset/Policy.csv'
file_4= '/home/pranjal/Desktop/spark_assignment/dataset/Region.csv'
file_5= '/home/pranjal/Desktop/spark_assignment/dataset/SearchTrend.csv'
file_6= '/home/pranjal/Desktop/spark_assignment/dataset/SeoulFloating.csv'
file_7= '/home/pranjal/Desktop/spark_assignment/dataset/Time.csv'
file_8= '/home/pranjal/Desktop/spark_assignment/dataset/TimeAge.csv'
file_9= '/home/pranjal/Desktop/spark_assignment/dataset/TimeGender.csv'
file_10= '/home/pranjal/Desktop/spark_assignment/dataset/TimeProvince.csv'
file_11= '/home/pranjal/Desktop/spark_assignment/dataset/Weather.csv'



# Kafka producer config
producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': ssl_mechanism,
    'sasl.username': api_key,
    'sasl.password': api_secret,
}
producer = Producer(producer_config)


# Function to generate messages
def generate_message(kafka_topic, file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,1:]
    for index, row in df.iterrows():
        message = json.dumps(row.to_dict()) 
        producer.produce(kafka_topic, value=message)
        print(f"Sending message to {kafka_topic}: {message}")

generate_message(kafka_topic5,file_5)

producer.flush()
print("Messages sent successfully")


