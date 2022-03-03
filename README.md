# ece419-project
UofT Distributed Systems Project

![example workflow](https://github.com/frederickjboyd/ece419-project/actions/workflows/ant.yml/badge.svg)

**Test Run**
To start ECS: `java -jar m2-ecs.jar ecs.config`

Then: `addnode FIFO 50`

Then: `start`

To start client: `java -jar m2-client.jar`

Then: `connect localhost <serverPort>`


**Shutdown Procedure (temp)**
ECS: `shutdown`

KVClient: `quit`

ECS: `quit`
