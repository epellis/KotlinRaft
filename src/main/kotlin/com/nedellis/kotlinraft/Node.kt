package com.nedellis.kotlinraft

import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

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

@ExperimentalCoroutinesApi
class Node(private val tk: Toolkit) {
    private val ctx = Contexts()
    private val fsm = FSM(ctx, ::blockingBecomeFollower, ::blockingBecomeCandidate, ::blockingBecomeLeader)
    private val log = Log()

    suspend fun run() = becomeFollower()

    suspend fun append(req: AppendRequest): AppendResponse {
        tk.logger.info("Append Request: $req, LOG: $log")
        if (req.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }
        return log.append(req)
    }

    suspend fun vote(req: VoteRequest): VoteResponse {
        tk.logger.info("Vote Request: $req, LOG: $log")
        if (req.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }
        return log.vote(req)
    }

    private suspend fun becomeFollower() {
        tk.logger.info("BecomeFollower")
        withContext(ctx.follower) {
            launch {
                delay(FOLLOWER_TIMEOUT)
                fsm.transition(Event.FollowerTimeout)
            }
        }
    }

    private suspend fun becomeCandidate() {
        tk.logger.info("BecomeCandidate")
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
        tk.logger.info("BecomeLeader")
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

    private suspend fun requestVotes(): Int {
        val results = channelFlow {
            tk.stubs.map { (client, info) ->
                launch {
                    send(requestVote(client, info))
                }
            }
        }

        return results.count { it == 1 }
    }

    // Return 1 if vote granted, else 0. Perform error checking on returned value
    private suspend fun requestVote(client: Int, info: PeerInfo): Int {
        tk.logger.info("Requesting vote from $client")
        val req = log.buildVoteRequest(tk.port)
        val res = info.raftStub.vote(req)

        if (res.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }

        return if (res.voteGranted) {
            1
        } else {
            0
        }
    }

    private suspend fun refreshFollowers() {
        coroutineScope {
            for ((client, info) in tk.stubs) {
                val newInfo = refreshFollower(client, info)
                tk.stubs[client] = newInfo
            }
        }
    }

    private suspend fun refreshFollower(client: Int, info: PeerInfo): PeerInfo {
        tk.logger.info("Refreshing follower $client")
        val req = log.buildAppendRequest(tk.port, 0, 0) // TODO
        val res = info.raftStub.append(req)

        if (res.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }

        // TODO: Reconcile log if follower is behind

        return info.copy()
    }

    // Exported to non suspending code
    private fun blockingBecomeFollower() = runBlocking { becomeFollower() }
    private fun blockingBecomeCandidate() = runBlocking { becomeCandidate() }
    private fun blockingBecomeLeader() = runBlocking { becomeLeader() }
}
