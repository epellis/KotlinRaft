package com.nedellis.kotlinraft

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import org.slf4j.Logger

// Essential utilities each actor needs
data class Toolkit(
    val logger: Logger,
    val port: Int,
    val raftStubs: Map<Int, RaftGrpcKt.RaftCoroutineStub>,
    val controlStubs: Map<Int, ControlGrpcKt.ControlCoroutineStub>
)

// Input and Output Actor
interface IOActor<in I, out O> {
    suspend fun run(inChan: ReceiveChannel<I>, outChan: SendChannel<O>)
}

// Input Actor
interface IActor<in I> {
    suspend fun run(inChan: ReceiveChannel<I>)
}
