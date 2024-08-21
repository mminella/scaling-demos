Docker MySQL configuration
https://www.datacamp.com/tutorial/set-up-and-configure-mysql-in-docker

Multithreaded Job
- Run in IDE

Parallel Steps
- Run in IDE

Async ItemProcessor/ItemWriter
- Run in IDE

Partitioning
- `java -jar partitioned-demo-0.0.1-SNAPSHOT.jar`
- `ps -ef | grep partition`

Remote Chunking
- Worker: `java -jar -Dspring.profiles.active=worker -Dspring.amqp.deserialization.trust.all=true remote-chunking-0.0.1-SNAPSHOT.jar`
- Manager: `java -jar -Dspring.profiles.active=manager -Dspring.amqp.deserialization.trust.all=true remote-chunking-0.0.1-SNAPSHOT.jar`

