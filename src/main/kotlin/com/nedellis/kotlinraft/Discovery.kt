package com.nedellis.kotlinraft

import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver

// Allows clients to discover one another
class Discovery(val port: Int) {
    val server = ServerBuilder.forPort(port)
        .addService(DiscoveryImpl())
        .build()

    fun run() {
        Thread { server.start().awaitTermination() }.start()
    }

    internal class DiscoveryImpl : DiscoveryGrpc.DiscoveryImplBase() {
        val connectedClients = mutableSetOf<Int>()

        override fun registerNewClient(
            request: RegisterNewClientRequest?,
            responseObserver: StreamObserver<RegisterNewClientResponse>?
        ) {
            if (request != null && responseObserver != null) {
                connectedClients.add(request.port)
                val reply = RegisterNewClientResponse.newBuilder().addAllClientPorts(connectedClients).build()
                responseObserver.onNext(reply)
                responseObserver.onCompleted()
            }
        }
    }
}
