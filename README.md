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

## Envoy
Unfortunately gRPC-web does not have in-process support for most langauges
so we have to use [Envoy](https://www.envoyproxy.io/) as a sidecare to proxy between web clients and our backend
service. To start envoy, run;
```bash
$ cd envoy

$ docker build -t epelesis/envoy -f ./envoy.Dockerfile .

$ docker run -d -p 8080:8080 epelesis/envoy
```
Since this section relies on the fine details of Docker networking, if you are not
on MacOS you might need to check on the 
[official tutorial](https://github.com/grpc/grpc-web/tree/master/net/grpc/gateway/examples/helloworld)
to solve issues with proxying.
