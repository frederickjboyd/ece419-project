# ece419-project
![example workflow](https://github.com/frederickjboyd/ece419-project/actions/workflows/ant.yml/badge.svg)

# UofT ECE419 Distributed Systems Project
---
## How to Run

*Note that Milestone 4 changes are built on M3 files:*

To start ECS: `java -jar m3-ecs.jar ecs.config`

Then select number of servers to add, cache type (FIFO/LRU/LFU), cache size:
`addnodes 3 FIFO 50`

Then put the servers in started state: `start`

To see the status of hash ranges/nodes, enter: `status`

Now, to start client: `java -jar m3-client.jar`

Then if on local machine: `connect localhost <serverPort>`

If connecting from a remote machine: `connect <serverIP> <serverPort>`

(You can use `hostname -i` to get the IP of the ECS machine)

---

## Shutdown Procedure and File Cleanup

ECS: 
`shutdown`
`cleanall`
`quit`

KVClient: `quit`
