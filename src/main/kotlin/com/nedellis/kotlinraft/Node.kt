package com.nedellis.kotlinraft


class Node {
    private val fsm = FSM()

    suspend fun append(req: AppendRequest): AppendResponse {}
    suspend fun vote(req: VoteRequest): VoteResponse {}
}