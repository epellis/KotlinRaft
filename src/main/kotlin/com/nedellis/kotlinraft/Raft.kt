package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch

class Raft(val port: Int, val discoveryPort: Int) {
    val discoveryChannel = ManagedChannelBuilder.forAddress("localhost", discoveryPort)
        .usePlaintext()
        .build()
    val discoveryStub = DiscoveryGrpc.newFutureStub(discoveryChannel)
    var clients = mutableSetOf<Int>()

    fun run() = GlobalScope.launch {
        val request = RegisterNewClientRequest.newBuilder().setPort(port).build()
        val response = discoveryStub.registerNewClient(request).await()
        clients = response.clientPortsList.toMutableSet()
    }
}
