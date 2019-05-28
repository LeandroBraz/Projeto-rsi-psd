from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
import lorem
import time
import csv

def lercsv(nome_arq, lista):
    arq = open(nome_arq + '.csv')
    lin = csv.DictReader(arq)
    
    for i in lin:
        info = (i['Info']).split(',')
        lista.append(i['Source'] + ', ' + i['Time'] + ',' + info[-1])

    arq.close()
 
# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: str(v).encode('utf-8'))



lista = []
lercsv('pcap0', lista)

lercsv('pcap1', lista)

lercsv('pcap2', lista)

lercsv('pcap3', lista)

lercsv('pcap4', lista)

lercsv('pcap5', lista)

lercsv('pcap6', lista)

# Call the producer.send method with a producer-record
print("ctrl+c to stop...")
'''
while True:
    producer.send('meu-topico-legal', lorem.sentence())
    time.sleep(10)'''

for i in lista:
    producer.send('meu-topico-legal', i)
    ime.sleep(10)
