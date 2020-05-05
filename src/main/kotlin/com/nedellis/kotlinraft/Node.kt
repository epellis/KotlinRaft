package com.nedellis.kotlinraft

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.*

const val FOLLOWER_TIMEOUT = 5000L
const val CANDIDATE_TIMEOUT = 1000L
const val LEADER_TIMEOUT = 1000L

data class Contexts(
    var follower: CoroutineContext = EmptyCoroutineContext,
    var candidate: CoroutineContext = EmptyCoroutineContext,
    var leader: CoroutineContext = EmptyCoroutineContext
) {
    fun refresh() {
        follower.cancel()
        candidate.cancel()
        leader.cancel()
        follower = EmptyCoroutineContext
        candidate = EmptyCoroutineContext
        leader = EmptyCoroutineContext
    }
}

class Node(private val tk: Toolkit) {
    private val ctx = Contexts()
    private val fsm = FSM(ctx, ::blockingBecomeFollower, ::blockingBecomeCandidate, ::blockingBecomeLeader)
    private val log = Log()

    suspend fun append(req: AppendRequest): AppendResponse {
        if (checkAppendIsHigherTerm(req)) {
            fsm.transition(Event.HigherTermServer)
        }
        return when (fsm.state) {
            State.Follower -> log.append(req)
            State.Candidate -> log.append(req)
            State.Leader -> log.append(req)
        }
    }

    suspend fun vote(req: VoteRequest): VoteResponse {
        if (checkVoteIsHigherTerm(req)) {
            fsm.transition(Event.HigherTermServer)
        }
        return when (fsm.state) {
            State.Follower -> log.vote(req)
            State.Candidate -> log.vote(req)
            State.Leader -> log.vote(req)
        }
    }

    private suspend fun becomeFollower() {
        withContext(ctx.follower) {
            launch {
                delay(FOLLOWER_TIMEOUT)
                fsm.transition(Event.FollowerTimeout)
            }
        }
    }

    private suspend fun becomeCandidate() {
        withContext(ctx.candidate) {
            launch {
                val votesReceived = requestVotes()
                TODO("Check if enough votes received")
            }

            launch {
                delay(CANDIDATE_TIMEOUT)
                fsm.transition(Event.CandidateTimeout)
            }
        }
    }

    private suspend fun becomeLeader() {
        withContext(ctx.leader) {
            launch {
                refreshFollowers()
            }

            launch {
                delay(LEADER_TIMEOUT)
                fsm.transition(Event.LeaderRefreshTimer)
            }
        }
    }

    private suspend fun checkAppendIsHigherTerm(req: AppendRequest): Boolean {
        TODO()
    }

    private suspend fun checkVoteIsHigherTerm(req: VoteRequest): Boolean {
        TODO()
    }

    private suspend fun requestVotes(): Int {
        TODO()
    }

    private suspend fun refreshFollowers() {
        TODO()
    }

    // Exported to non suspending code
    private fun blockingBecomeFollower() = runBlocking { becomeFollower() }
    private fun blockingBecomeCandidate() = runBlocking { becomeCandidate() }
    private fun blockingBecomeLeader() = runBlocking { becomeLeader() }
}
