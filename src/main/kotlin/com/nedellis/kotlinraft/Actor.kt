package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.Logger

data class PeerInfo(val raftStub: RaftGrpcKt.RaftCoroutineStub, val controlStub: ControlGrpcKt.ControlCoroutineStub)

fun buildPeer(port: Int): PeerInfo {
    val raftStub = RaftGrpcKt.RaftCoroutineStub(
        ManagedChannelBuilder.forAddress("localhost", port)
            .usePlaintext()
            .executor(Dispatchers.IO.asExecutor())
            .build()
    )

    val controlStub = ControlGrpcKt.ControlCoroutineStub(
        ManagedChannelBuilder.forAddress("localhost", port)
            .usePlaintext()
            .executor(Dispatchers.IO.asExecutor())
            .build()
    )

    return PeerInfo(raftStub, controlStub)
}

// Essential utilities each actor needs
data class Toolkit(
    val logger: Logger,
    val port: Int,
    val stubs: Map<Int, PeerInfo>
)

// Input and Output Actor
interface IOActor<in I, out O> {
    suspend fun run(inChan: ReceiveChannel<I>, outChan: SendChannel<O>)
}

// Input Actor
interface IActor<in I> {
    suspend fun run(inChan: ReceiveChannel<I>)
}
