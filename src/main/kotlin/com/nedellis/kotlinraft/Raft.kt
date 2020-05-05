package com.nedellis.kotlinraft

import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val stubs = clients.filter { it != port }.map { it to buildPeer(it) }.toMap()
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

// private class ControlService(
//    private val actor: SendChannel<MetaRpc>,
//    private val logger: Logger? = null
// ) :
//    ControlGrpcKt.ControlCoroutineImplBase() {
//    override suspend fun getEntry(request: Key): GetStatus {
//        val res = CompletableDeferred<GetStatus>()
//        actor.send(MetaRpc.RpcWrapper(Rpc.GetEntry(request, res)))
//        return res.await()
//    }
//
//    override suspend fun updateEntry(request: Entry): UpdateStatus {
//        val res = CompletableDeferred<UpdateStatus>()
//        actor.send(MetaRpc.RpcWrapper(Rpc.UpdateEntry(request, res, logger)))
//        return res.await()
//    }
//
//    override suspend fun idleClient(request: Nothing): Nothing {
//        actor.send(MetaRpc.Idle)
//        return Nothing.newBuilder().build()
//    }
//
//    override suspend fun wakeClient(request: Nothing): Nothing {
//        actor.send(MetaRpc.Wake)
//        return Nothing.newBuilder().build()
//    }
// }
