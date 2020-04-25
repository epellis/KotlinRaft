# KotlinRaft

Provides a replicated, fault tolerant key-value storage that can be used across the network in face of partitions and machine crashes. In other words, everything that the [Raft Paper](https://raft.github.io/raft.pdf) garauntees.

Currently, this project will just provide the replicated key-value storage mechanism and some basic configuration settings but if time permits I might expand it to use a real service coordinator like Etcd/Consul or perform distributed data calculations like Apache Spark.

Internally, this project uses coroutines and GRPC to operate asynchronously and performantly.

## Build Instructions
```bash
# Build
$ ./gradlew build

# Run
$ ./gradlew run

# Jar (Found in ./build/libs/)
$ ./gradlew shadowJar
```

## gRPC Web Proxy
In order to connect the dashboard to the raft cluster we will need to run an intermediary proxy that translates
regular gRPC to gRPC-web. [Download an executable](https://github.com/improbable-eng/grpc-web/releases) and
run with the following commands:
```bash
./grpcwebproxy --backend_addr=localhost:8000 --run_tls_server=false
```
