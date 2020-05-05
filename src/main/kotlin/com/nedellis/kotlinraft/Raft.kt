package com.nedellis.kotlinraft

import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val stubs = clients.filter { it != port }.map { it to buildPeer(it) }.toMap().toMutableMap()
    private val logger = LoggerFactory.getLogger("Raft $port")
    private val tk = Toolkit(logger, port, stubs)

    private val node = Node(tk)

    suspend fun run() = coroutineScope {
        launch(Dispatchers.IO) {
            ServerBuilder.forPort(port)
                .addService(RaftService(node))
//                .addService(ControlService(gRPCtoCoordinatorChan, logger))
                .executor(Dispatchers.IO.asExecutor())
                .build()
                .start()
                .awaitTermination()
        }
    }
}

private class RaftService(private val node: Node) :
    RaftGrpcKt.RaftCoroutineImplBase() {
    override suspend fun append(request: AppendRequest): AppendResponse {
        return node.append(request)
    }

    override suspend fun vote(request: VoteRequest): VoteResponse {
        return node.vote(request)
    }
}
