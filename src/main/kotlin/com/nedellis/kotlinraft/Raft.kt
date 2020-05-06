package com.nedellis.kotlinraft

import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val stubs = clients.filter { it != port }.map { it to buildPeer(it) }.toMap().toMutableMap()
    private val logger = LoggerFactory.getLogger("Raft $port")
    private val tk = Toolkit(logger, port, stubs)

    @ExperimentalCoroutinesApi
    private val node = Node(tk)

    @ExperimentalCoroutinesApi
    suspend fun run() = coroutineScope {
        launch(Dispatchers.IO) {
            ServerBuilder.forPort(port)
                .addService(RaftService(node))
                .addService(ControlService(node))
                .executor(Dispatchers.IO.asExecutor())
                .build()
                .start()
                .awaitTermination()
        }

        launch(Dispatchers.IO) {
            node.run()
        }
    }
}

private class RaftService(private val node: Node) : RaftGrpcKt.RaftCoroutineImplBase() {
    @ExperimentalCoroutinesApi
    override suspend fun append(request: AppendRequest): AppendResponse {
        return node.append(request)
    }

    @ExperimentalCoroutinesApi
    override suspend fun vote(request: VoteRequest): VoteResponse {
        return node.vote(request)
    }
}

private class ControlService(private val node: Node) : ControlGrpcKt.ControlCoroutineImplBase() {
    @ExperimentalCoroutinesApi
    override suspend fun getEntry(request: Key): GetStatus {
        return node.getEntry(request)
    }

    @ExperimentalCoroutinesApi
    override suspend fun updateEntry(request: Entry): UpdateStatus {
        return node.updateEntry(request)
    }
}
