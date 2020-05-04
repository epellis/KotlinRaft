package com.nedellis.kotlinraft

data class Vote(private val votedFor: Int? = null) {
    fun vote(req: VoteRequest): VoteResponse {
        TODO()
    }
}