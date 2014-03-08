#!/usr/bin/env python
import pika
import json
import sys
import random

id = sys.argv[1]
tp = random.randint(1,3)

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='all_event', durable=True)

d = {'id': id, 'type': tp, 'name': 'Ye Myat', 'university': 'Uni Bonn', 'degree': 'Master', 'start': 2012}
dstr = json.dumps(d)

channel.basic_publish(exchange='', routing_key='all_event', body=dstr, 
			properties=pika.BasicProperties(delivery_mode=2))
print 'Sent %s!' % id

connection.close()
