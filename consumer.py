from confluent_kafka import Consumer, KafkaError
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import quote_plus
import json





# Kafka consumer config
api_key = 'EQT36REMCPAUFDZ3'
api_secret = 'EmAA2ZxHdJQ8YtnjhlwBl6TUzhKTIBWB2AheQorWrukBBt5NYRTZtm26YFf5endY'
bootstrap_servers = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
security_protocol = 'SASL_SSL'
ssl_mechanism = 'PLAIN'

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': security_protocol,
    'sasl.mechanism': ssl_mechanism,
    'sasl.username': api_key,
    'sasl.password': api_secret,
}

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


# Mongodb atlas collections

collection_name1= 'case'
collection_name2= 'patient_info'
collection_name3= 'policy'
collection_name4= 'region'
collection_name5= 'search_trend'
collection_name6= 'seoul_floating'
collection_name7= 'time'
collection_name8= 'time_age'
collection_name9= 'time_gender'
collection_name10= 'time_province'
collection_name11= 'weather'



# MongoDB client and database config

username = quote_plus('pranjal_tripathi')
password = quote_plus('Pranjal_01')
cluster = 'spark-project.9yp16zi.mongodb.net'

uri = 'mongodb+srv://' + username + ':' + password + '@' + cluster + '/?retryWrites=true&w=majority'

client = MongoClient(uri, server_api=ServerApi('1'))

db = client['spark_project']



#Creating a Kafka consumer
consumer_config.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})
consumer = Consumer(consumer_config)
consumer.subscribe([kafka_topic11])
print(f"Kafka topic {kafka_topic11} is subscribed")






def consume_messages(collection_name):
    collection = db[collection_name]
    print(f"Inside {collection_name} Collection")

    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            print("Consumer error: {}".format(message.error()))
            continue
        data = json.loads(message.value().decode("utf-8"))
        print(f"writing to {collection}:", data)
        collection.insert_many([data])
        print(f"Message written to MongoDB {collection_name} collection", end="")
 



consume_messages(collection_name11)
