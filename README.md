# About this repository

This repository contains demo applications presented at
[Spring One 2018 session: High Performance Batch Processing](https://springoneplatform.io/2018/sessions/high-performance-batch-processing).

The base application we are going to scale is defined in `io.spring.batch.scalingdemos.singlethreaded.SinglethreadedJobApplication`.
It loads transactions data from a flat file into a relational database table.

# Pre-requisites

* Java 8+
* Maven 3+
* MySQL server up and running on port `3306`. If you use docker, you can run: `docker run -e MYSQL_ROOT_PASSWORD=root -d -p 3306:3306 mysql`

Each demo will insert data in the `TRANSACTION` table as well as in Spring Batch
meta-data tables. In order to run each demo on a fresh database, you can run the
script `schema.sql` *before* running each demo. For example using the [MySQL shell](https://dev.mysql.com/downloads/shell/):

```
>mysqlsh root@localhost:3306
>MySQL  localhost:3306 ssl  JS > \sql
Switching to SQL mode... Commands end with ;

>MySQL  localhost:3306 ssl  SQL > \source '/full/path/to/scaling-demos/schema.sql';
```

# Single JVM demos

## Multi-Threaded step

Run `io.spring.batch.scalingdemos.singlethreaded.SinglethreadedJobApplication`
from within your IDE without any argument.

## Parallel steps

Run `io.spring.batch.scalingdemos.parallel.ParallelStepsJobApplication`
from within your IDE without any argument.

## Asynchronous item processor/writer

Run `io.spring.batch.scalingdemos.asyncprocessor.AsyncProcessorJobApplication` 
from within your IDE without any argument.

# Multiple JVMs demos

## Pre-requisites

In addition to pre-requisites of single JVMs demos, you will need to have a RabbitMQ
server up and running on port `5671`. If you use docker, you can run: `docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management`
You need to create two durable queues called `requests` and `replies`. You can do
this from the management console available at `localhost:15672` with credentials: `guest/guest`.

## Remote partitioning

```
$>cd partitioned-demo
$>mvn clean package
```

Run each of the following commands in a separate terminal:

* Run a first worker step: `java -jar target/partitioned-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker`
* Run a second worker step: `java -jar target/partitioned-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker`
* Run the master step: `java -jar target/partitioned-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=master`

## Remote chunking

```
$>cd remote-chunking
$>mvn clean package
```

Run each of the following commands in a separate terminal:

* Run a first worker: `java -jar target/remote-chunking-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker`
* Run a second worker: `java -jar target/remote-chunking-0.0.1-SNAPSHOT.jar --spring.profiles.active=worker`
* Run the master step: `java -jar target/remote-chunking-0.0.1-SNAPSHOT.jar --spring.profiles.active=master`
