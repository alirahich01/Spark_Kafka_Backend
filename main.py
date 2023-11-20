from random import randint
from typing import Set, Any
from fastapi import FastAPI
from kafka import TopicPartition
from fastapi import WebSocket
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
from typing import Optional
import matplotlib.pyplot as plt
import pickle
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import *

import pandas as pd
import uvicorn
import aiokafka
import asyncio
import json
import logging
import os

# instantiate the API
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ourPred = []
spark = SparkSession.builder.appName("log_regconsulte").getOrCreate()

model = DecisionTreeClassificationModel.load("inputTestModel")
#model = BinaryClassificationEvaluator.load('Model')
# global variables
arguments = None
resultat_pred = None
consumer_task = None
consumer = None
_state = 0
df = pd.read_csv('consumerfile.csv')
to_describe = pd.DataFrame(
    columns=['Names', 'Age', 'Total_Purchase', 'Account_Manager', 'Years', 'Num_Sites', 'Location', 'Company', 'Churn'])
# env variables
KAFKA_TOPIC = 'TopicSpark'
KAFKA_CONSUMER_GROUP_PREFIX = os.getenv('KAFKA_CONSUMER_GROUP_PREFIX', 'group')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9092')

# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',level=logging.INFO)
log = logging.getLogger(__name__)


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await consume()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    consumer_task.cancel()
    await consumer.stop()


@app.get("/loadcsv")
def load_csv():
    async def iter_df(): 
        #fct pour gere le flux de donne json une ligne à la fois
        for _, res in df.iterrows():
            await asyncio.sleep(1)
            row1 = json.dumps(res.to_dict()) 
            #pour convertir chaque ligne en un dictionnaire JSON
            row = json.loads(row1) 
            #pour charger le dictionnaire JSON résultant
            tp = [str(row['Names']), int(row['Age']), float(row['Total_Purchase']), int(row['Account_Manager']),int(row['Years']), 
                row['Num_Sites'], str(row['Location']), str(row['Company']), int(row['Churn'])]
            described = await csvproduce(tp)
            yield described 
#Utilise yield pour retourner chaque résultat de la fonction csvproduce
# au client de manière asynchrone, formant ainsi un flux continu de données.
    return StreamingResponse(iter_df(), media_type="application/json")
#Crée une réponse de streaming en utilisant la fonction StreamingResponse.
# Elle prend en paramètre la fonction asynchrone iter_df() et spécifie que le type de média est "application/json".
# Cela permet d'envoyer les données en continu au client sous forme de flux JSON.


#class patient(BaseModel):
#    names: str
#    age: DoubleType
#    total_Purchase: DoubleType
#    account_Manager: int
#    years: DoubleType
#    num_Sites: DoubleType
#    location: str
#    company: str


class patient(BaseModel):
    names: str
    age: Optional[int]
    total_Purchase: Optional[float]
    account_Manager: Optional[int]
    years: Optional[int]
    num_Sites: Optional[float]
    location: str
    company: str


@app.post("/predict/")
async def create_item(p: patient):
    await produce(p)
    return p


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/state")
async def state():
    return {"state": _state}


async def initialize():
    loop = asyncio.get_event_loop()
    global consumer 
#Déclare une variable globale consumer pour stocker le consommateur Kafka.
    group_id = f'{KAFKA_CONSUMER_GROUP_PREFIX}-{randint(0, 10000)}'
#Génère un identifiant de groupe Kafka unique en ajoutant un suffixe aléatoire au préfixe spécifié.
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_TOPIC}, group_id {group_id}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    consumer = aiokafka.AIOKafkaConsumer(KAFKA_TOPIC, loop=loop,bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,group_id=group_id)
#Crée un consommateur Kafka en utilisant la classe aiokafka.AIOKafkaConsumer().
# La classe aiokafka.AIOKafkaConsumer() permet de consommer des messages Kafka de manière asynchrone.
# get cluster layout and join group
    await consumer.start()
#Démarre le consommateur Kafka de manière asynchrone.

    partitions: Set[TopicPartition] = consumer.assignment()
#La méthode assignment() renvoie un ensemble de partitions qui sont attribuées au consommateur.
    nr_partitions = len(partitions)
#Calcule le nombre de partitions attribuées au consommateur.
    if nr_partitions != 1:
#Avertissement si le nombre de partitions n'est pas égal à 1, car le script s'attend à n'avoir qu'une seule partition.
        log.warning(f'Found {nr_partitions} partitions for topic {KAFKA_TOPIC}. Expecting '
                    f'only one, remaining partitions will be ignored!')
    
    for tp in partitions:

        # get the log_end_offset
        end_offset_dict = await consumer.end_offsets([tp])
#La méthode end_offsets() renvoie un dictionnaire qui mappe les partitions aux décalages de fin du journal.
        end_offset = end_offset_dict[tp]
        
        if end_offset == 0:
            log.warning(f'Topic ({KAFKA_TOPIC}) has no messages (log_end_offset: '
                        f'{end_offset}), skipping initialization ...')
            return

        log.debug(f'Found log_end_offset: {end_offset} seeking to {end_offset - 1}')
        consumer.seek(tp, end_offset - 1)
        
        msg = await consumer.getone()
        print("#################################")
        
# La méthode getone() renvoie le prochain message disponible dans le consommateur.
        print("-------------------------")
        log.info(f'Initializing API with data from msg: {msg}')

        # update the API state
        # _update_state(msg)
        return


@app.get("/consume")
async def consume():
    global consumer_task
    global arguments
    consumer_task = asyncio.create_task(send_consumer_message(consumer))
    #await send_consumer_message(consumer)
    return {"résultat": resultat_pred}


@app.get("/stopconsumer")
async def stopconsumer():
    consumer.stop()


async def send_consumer_message(consumer):
    loop = asyncio.get_event_loop()
    global arguments
    print(arguments)
    global resultat_pred
    print(resultat_pred)
    
    print("-------------------------------")
    try:
        async for msg in consumer:
            log.info(f"Consumed msg: {msg}")
            arguments = json.loads(msg.value.decode())
            To_Predict = Vectors.dense(arguments)
            val = model.predict(To_Predict)
            
            print("##################################################################")
            print(To_Predict)
            
            if val == 1:
                resultat_pred = "fidèle"
            else:
                resultat_pred = "non fidèle"
            ourPred = []
    except json.JSONDecodeError as json_error:
        log.error(f"JSON decoding error: {json_error}")
        # Ajoutez ici la gestion de l'erreur de décodage JSON
    except Exception as e:
        log.error(f"An error occurred: {e}")
        # Ajoutez ici la gestion des autres erreurs
        
    finally:
        log.warning('Stopping consumer')
    loop.run_until_complete(consume())


#def _update_state(message: Any) -> None:
#    value = json.loads(message.value)
#    global _state
#    _state = value['state']


async def produce(p: patient):
#Cette fonction asynchrone produce est responsable de la production de messages vers le topic Kafka
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
# get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        ourPred = [p.names, p.age, p.total_Purchase, p.account_Manager, p.years, p.num_Sites, p.location, p.company]
        value_json = json.dumps(ourPred).encode('utf-8')
        await producer.send_and_wait(KAFKA_TOPIC, value_json)
        print("{} Produced".format(time.time()))
        #ourPred = []
        time.sleep(3)
    finally:
# wait for all pending messages to be delivered or expire.
        await producer.stop()

"""
async def csvproduce(row: []):
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    # get cluster layout and initial topic/partition leadership information
    await producer.start()
    
    try:
        to_describe.loc[len(to_describe)] = row
        Describe = {"Means": {"Names_Mean": to_describe["Names"].mean(),
                              "Age_Mean": to_describe["Age"].mean(),
                              "Total_Purchase_Mean": to_describe["Total_Purchase"].mean(),
                              "Account_Manager_Mean": to_describe["Account_Manager"].mean(),
                              "Years": to_describe["Years"].mean(),
                              "Num_Sites_Mean": to_describe["Num_Sites"].mean(),
                              #"Location_Mean": to_describe["Location"].mean(),
                              #"Company_Mean": to_describe["Company"].mean(),
                              "Churn_Mean": to_describe["Churn"].mean()
                              },
                    "Maxs": {"Names_Max": str(to_describe["Names"].max()),
                             "Age_Max": Optional[int](to_describe["Age"].max()),
                             "Total_Purchase_Max":Optional[float](to_describe["Total_Purchase"].max()),
                             "Account_Manager_Max": int(to_describe["Account_Manager"].max()),
                             "Years_Max":Optional[int](to_describe["Years"].max()),
                             "Num_Sites_Max":Optional[float](to_describe["Num_Sites"].max()),
                             #"Location_Max": str(to_describe["Location"].max()),
                             #"Company_Max": str(to_describe["Company"].max()),
                             "Churn_Max": Optional[int](to_describe["Churn"].max())
                             },
                    "Mins": {"Names_Min": str(to_describe["Names"].min()),
                             "Age_Min": Optional[int](to_describe["Age"].min()),
                             "Total_Purchase_Min": Optional[float](to_describe["Total_Purchase"].min()),
                             "Account_Manager_Min": int(to_describe["Account_Manager"].min()),
                             "Years_Min": Optional[int](to_describe["Years"].min()),
                             "Num_Sites_Min": Optional[float](to_describe["Num_Sites"].min()),
                             #"Location_Min": str(to_describe["Location"].min()),
                             #"Company_Min": str(to_describe["Company"].min()),
                             "Churn_Min": Optional[int](to_describe["Churn"].min())
                             },
                    "Counts": {"Names_Count": str(to_describe["Names"].count()),
                               "Age_Count": Optional[int](to_describe["Age"].count()),
                               "Total_Purchase_Count": Optional[float](to_describe["Total_Purchase"].count()),
                               "Account_Manager_Count": int(to_describe["Account_Manager"].count()),
                               "Years_Count": Optional[int](to_describe["Years"].count()),
                               "Num_Sites_Count": Optional[float](to_describe["Num_Sites"].count()),
                               #"Location_Count": str(to_describe["Location"].count()),
                               #"Company_Count": str(to_describe["Company"].count()),
                               "Churn_Count": Optional[int](to_describe["Churn"].count())
                               }}
                        
# yield json.dumps(Describe).encode('utf-8') + '\n'
        value_json = json.dumps(Describe,indent=4).encode('utf-8')
        print(value_json)
        await producer.send_and_wait(KAFKA_TOPIC, value_json)
        print("{} Produced  ".format(time.time()))
        time.sleep(3)
        return value_json
    finally:
# wait for all pending messages to be delivered or expire.
        await producer.stop()
"""

async def csvproduce(row: []):
    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    # get cluster layout and initial topic/partition leadership information
    await producer.start()

    try:
        to_describe.loc[len(to_describe)] = row
        
        # Convertir les colonnes spécifiées en nombres si possible
        numeric_columns = ["Names","Age", "Total_Purchase", "Account_Manager", "Years", "Num_Sites", "Churn"]
        to_describe[numeric_columns] = to_describe[numeric_columns].apply(pd.to_numeric, errors="coerce")
        
        Describe = {
            "Means": {
                #"Names_Mean": to_describe["Names"].mean(),
                "Age_Mean": to_describe["Age"].mean(),
                "Total_Purchase_Mean": to_describe["Total_Purchase"].mean(),
                "Account_Manager_Mean": to_describe["Account_Manager"].mean(),
                "Years": to_describe["Years"].mean(),
                "Num_Sites_Mean": to_describe["Num_Sites"].mean(),
                #Location_Mean": to_describe["Location"].mean(),
                #"Company_Mean": to_describe["Company"].mean(),
                "Churn_Mean": to_describe["Churn"].mean()
            },
            "Maxs": {
                #"Names_Max": str(to_describe["Names"].max()),
                "Age_Max": int(to_describe["Age"].max()) if not pd.isnull(to_describe["Age"].max()) else pd.NA,
                "Total_Purchase_Max": float(to_describe["Total_Purchase"].max()) if not pd.isnull(to_describe["Total_Purchase"].max()) else pd.NA,
                "Account_Manager_Max": int(to_describe["Account_Manager"].max()),
                "Years_Max": int(to_describe["Years"].max()) if not pd.isnull(to_describe["Years"].max()) else pd.NA,
                "Num_Sites_Max": float(to_describe["Num_Sites"].max()) if not pd.isnull(to_describe["Num_Sites"].max()) else pd.NA,
                "Churn_Max": int(to_describe["Churn"].max()) if not pd.isnull(to_describe["Churn"].max()) else pd.NA
            },
            "Mins": {
                #"Names_Min": str(to_describe["Names"].min()),
                "Age_Min": int(to_describe["Age"].min()) if not pd.isnull(to_describe["Age"].min()) else pd.NA,
                "Total_Purchase_Min": float(to_describe["Total_Purchase"].min()) if not pd.isnull(to_describe["Total_Purchase"].min()) else pd.NA,
                "Account_Manager_Min": int(to_describe["Account_Manager"].min()),
                "Years_Min": int(to_describe["Years"].min()) if not pd.isnull(to_describe["Years"].min()) else pd.NA,
                "Num_Sites_Min": float(to_describe["Num_Sites"].min()) if not pd.isnull(to_describe["Num_Sites"].min()) else pd.NA,
                "Churn_Min": int(to_describe["Churn"].min()) if not pd.isnull(to_describe["Churn"].min()) else pd.NA
            },
            "Counts": {
                #"Names_Count": int(to_describe["Names"].count()),
                "Age_Count": int(to_describe["Age"].count()) if not pd.isnull(to_describe["Age"].count()) else pd.NA,
                "Total_Purchase_Count": int(to_describe["Total_Purchase"].count()) if not pd.isnull(to_describe["Total_Purchase"].count()) else pd.NA,
                "Account_Manager_Count": int(to_describe["Account_Manager"].count()),
                "Years_Count": int(to_describe["Years"].count()) if not pd.isnull(to_describe["Years"].count()) else pd.NA,
                "Num_Sites_Count": int(to_describe["Num_Sites"].count()) if not pd.isnull(to_describe["Num_Sites"].count()) else pd.NA,
                "Churn_Count": int(to_describe["Churn"].count()) if not pd.isnull(to_describe["Churn"].count()) else pd.NA
            }
        }
        
        value_json = json.dumps(Describe, indent=4).encode('utf-8')
        print(value_json)
        await producer.send_and_wait(KAFKA_TOPIC, value_json)
        print("{} Produced  ".format(time.time()))
        time.sleep(3)
        return value_json
    finally:
        # wait for all pending messages to be delivered or expire.
        await producer.stop()




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9092)
