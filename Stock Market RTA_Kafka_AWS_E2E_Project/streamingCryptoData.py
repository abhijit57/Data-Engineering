## This program triggers the WazirX API to get the real-time Crypto data. WazirX is an Indian cryptocurrency exchange.
## usage: python/python3 streamingCryptoData.py


# Importing necessary libraries
import pandas as pd
import numpy as np
import asyncio, websockets, requests
import os, sys, random, json, logging

from kafka import KafkaProducer, KafkaConsumer
from s3fs import S3FileSystem
from time import sleep
from json import dumps, loads

from wazirx_sapi_client.rest import Client
from wazirx_sapi_client.websocket import WebsocketClient

from tqdm import tqdm
from configparser import ConfigParser
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

# Reading Config File for fetching the Wazirx API keys
cfg_file = 'Config.ini'
config = ConfigParser(interpolation=None)
config.read(cfg_file, encoding='utf-8')
# Storing the contents of the config file into respective dictionary variables
wazirx_keys = dict(config.items('wazirx-keys'))


# Setting logging configurations
# logging.basicConfig(format='%(levelname)s:%(message)s',
#                     level=logging.DEBUG)


# Kafka Producer Initialization
producer = KafkaProducer(bootstrap_servers=['18.119.158.111:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


# Kafka Consumer Initialization
consumer = KafkaConsumer('crypto_stream',
                         bootstrap_servers=['18.119.158.111:9092'],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))


# Amazon S3 File System Initialization
s3 = S3FileSystem()


# Extract tickers data and convert it into a dataframe
def extract_tickers(conn):
    # Extract Data using the WazirX API
    tickers = conn.send('tickers')[1]
    # Transform the Extracted Data
    df = pd.DataFrame(tickers)
    # df['openPrice'] = df['openPrice'].astype(float)
    # df['lowPrice'] = df['lowPrice'].astype(float)
    # df['highPrice'] = df['highPrice'].astype(float)
    # df['lastPrice'] = df['lastPrice'].astype(float)
    # df['volume'] = df['volume'].astype(float)
    # df['bidPrice'] = df['bidPrice'].astype(float)
    # df['askPrice'] = df['askPrice'].astype(float)
    # df['datetime'] = pd.to_datetime(df['at'], unit='ms')

    return df


if __name__ == "__main__":

    # Testing the API connection
    client = Client(api_key=wazirx_keys['api_key'], secret_key=wazirx_keys['secret_key'])
    print('Pinging WazirX Server.......')
    if client.send("ping")[0] == 200:
        # logging.info('Ping Successful')
        print('Ping Successful')
    else:
        # logging.error('Ping Unsuccessful; Check the Authentication')
        print('Ping Unsuccessful; Check the Authentication')
        sys.exit(1)

    data = extract_tickers(client)
    print()
    # logging.info(f'Number of Records Extracted and Transformed: {len(data)}')
    print(f'Number of Records Extracted and Transformed: {len(data)}')

    print('\n', '--------------- Kafka Producer (Streaming Started) ------------------')
    # producer.flush()
    # while True:
    cnt = 0
    while cnt < len(data):
        dict_tickers = data.sample(1).to_dict(orient="records")[0]
        producer.send('crypto_stream', value=dict_tickers)
        cnt += 1
    print('--------------- Kafka Producer (Streaming Completed) ------------------')

    # print('\n', '**************** Streaming Data Uploading to S3 Started *******************')
    # # Writing message stream received in Kafka Consumer to Amazon S3
    # for count, i in enumerate(consumer):
    #     with s3.open("s3://crypto-real-time-kafka-project/test_rta_{}.json".format(count), 'w') as file:
    #         json.dump(i.value, file)
    # print('****************** Streaming Data Uploading to S3 Finished *******************')
