# ece419-project
![example workflow](https://github.com/frederickjboyd/ece419-project/actions/workflows/ant.yml/badge.svg)

UofT Distributed Systems Project

**Test Run**

To start ECS: `java -jar m3-ecs.jar ecs.config`

Then select number of servers to add, cache type, cache size:
`addnodes 3 FIFO 50`

Then: `start`

To start client: `java -jar m3-client.jar`

Then if on local machine: `connect localhost <serverPort>`

If connecting from a remote machine: `connect <serverIP> <serverPort>`

**Shutdown Procedure (temp)**

ECS: 
`shutdown`
`cleanall`
`quit`

KVClient: `quit`
