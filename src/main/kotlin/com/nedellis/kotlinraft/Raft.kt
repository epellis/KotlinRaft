package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch

class Raft(val port: Int, val discoveryPort: Int) {
    private val discoveryChannel = ManagedChannelBuilder.forAddress("localhost", discoveryPort)
        .usePlaintext()
        .build()
    private val discoveryStub = DiscoveryGrpc.newFutureStub(discoveryChannel)
    private var clients = mutableSetOf<Int>()
    private val messageChannel = Channel<Int>(Channel.UNLIMITED)

    val table = mutableMapOf<String, String>()

    private suspend fun registerAndGetClients(): MutableSet<Int> {
        val request = RegisterNewClientRequest.newBuilder().setPort(port).build()
        val response = discoveryStub.registerNewClient(request).await()
        return response.clientPortsList.toMutableSet()
    }

    // Use channel to talk between thread and coroutines
    // Make an actor system to coordinate, similar to event loop

    fun run() = GlobalScope.launch {
        clients = registerAndGetClients()

        // Debug coroutine
        launch {
            while (true) {
                delay(1000L)
                println("Clients of $port: $clients")
            }
        }

        // Event loop coroutine, reads from GRPC received message channel
        launch {
            while (true) {
                val message = messageChannel.receive()
            }
        }
    }
}
