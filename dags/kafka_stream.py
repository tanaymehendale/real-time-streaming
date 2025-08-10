import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaProducer
import time
import logging
import requests

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

default_args = {
    'owner': 'tanay_mehendale',
    'start_date': datetime(2025, 7, 9, 17, 00)
}

def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers="broker:29092", client_id="airflow")
    try:
        admin.create_topics([NewTopic("users_created", num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    logging.info("Entered stream_data()")
    
    ensure_topic()
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'], 
        acks='all',
        retries=3,
        linger_ms=10,
        max_block_ms=5000,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info(f"Finished KafkaProducer: {producer}")
    # curr_time = time.time()
    end_at = time.time() + 60
    logging.info("Kafka Producer successful")

    # while True:
    while time.time() < end_at:
        # if time.time() > curr_time + 60: #1 minute
        #     break
        try:
            res = format_data(get_data())
            logging.info("format_data() executed .... ")

            f = producer.send('users_created', res)
            logging.info("send() executed..... ")
            f.get(timeout=5)
        except Exception as e:
            logging.error(f'A producer Error occured: {e}')
            continue
    
    producer.flush()
    logging.info("Flushed all Kafka messages")
    return "Completed stream_data() run"

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    logging.info("Entered DAG......")
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data,
        dag = dag,
    )