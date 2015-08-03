#Rabbit-HDFS-Transporter

Components for Menthal app (https://menthal.org) batch layer. How to import data events submitted on the RabbitMQ message queue into HDFS file system. This uses Apache Flume to build a pipe line which accepts data and write to HDFS file system. Not a complete source code for use - just for use myself.

##Event simulation

(Assuming that RabbitMQ server is running)
sample_event_gen.py can be used as
  `for i in {1..100}:do python sample_event_gen.py $i;done`
  
