package com.nedellis.kotlinraft

fun main() {
    val discoveryServer = Discovery(8000)
    discoveryServer.run()

    val clients = (0..10).map {
        Raft(8001 + it, 8000)
    }

    for (client in clients) {
        client.run()
    }
}

