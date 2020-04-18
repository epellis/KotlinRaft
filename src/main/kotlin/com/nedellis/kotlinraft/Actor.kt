package com.nedellis.kotlinraft

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*

// Input and Output Actor
interface IOActor<in I, out O> {
    suspend fun run(inChan: ReceiveChannel<I>, outChan: SendChannel<O>)
}

// Input Actor
interface IActor<in I> {
    suspend fun run(inChan: ReceiveChannel<I>)
}
