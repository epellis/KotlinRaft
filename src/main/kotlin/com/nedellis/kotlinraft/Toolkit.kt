package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
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
