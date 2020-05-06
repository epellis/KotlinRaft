package com.nedellis.kotlinraft

import com.google.protobuf.Empty
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlin.coroutines.coroutineContext

const val FOLLOWER_TIMEOUT = 5000L
const val CANDIDATE_DELAY_MAX = 3000L
const val CANDIDATE_TIMEOUT = 1000L
const val LEADER_TIMEOUT = 1000L

@ExperimentalCoroutinesApi
class Node(private val tk: Toolkit) {
    private var ctx: Job? = null
    private val fsm = FSM(::changeState)
    private val log = Log(logger = tk.logger)
    private val changeStateChan = Channel<SideEffect>(Channel.CONFLATED) // Last write wins

    suspend fun run() = coroutineScope {
        // Event loop listens for next state change event
        launch {
            while (true) {
                val effect = changeStateChan.receive()
                ctx?.cancel()
                ctx = when (effect) {
                    SideEffect.BecomeFollower -> becomeFollower()
                    SideEffect.BecomeCandidate -> becomeCandidate()
                    SideEffect.BecomeLeader -> becomeLeader()
                }
            }
        }

        // Kick start as follower
        changeStateChan.send(SideEffect.BecomeFollower)
    }

    private fun changeState(effect: SideEffect): Boolean = changeStateChan.offer(effect)

    suspend fun append(req: AppendRequest): AppendResponse {
        tk.logger.info("Append Request: $req, LOG: $log")
        if (req.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }
        return log.append(req).also {
            tk.logger.info("Returning Append Request: $it")
        }
    }

    suspend fun vote(req: VoteRequest): VoteResponse {
        tk.logger.info("Vote Request: $req, LOG: $log")
        if (req.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }
        return log.vote(req).also {
            tk.logger.info("Returning Vote: $it")
        }
    }

    private suspend fun becomeFollower(): Job = coroutineScope {
        return@coroutineScope launch {
            tk.logger.info("BecomeFollower")
            delay(FOLLOWER_TIMEOUT)
            tk.logger.info("Follower Timed Out")
            fsm.transition(Event.FollowerTimeout)
        }
    }

    private suspend fun becomeCandidate(): Job = coroutineScope {
        return@coroutineScope launch {
            tk.logger.info("BecomeCandidate")
            log.changeTerm(log.term() + 1)
            delay((0L..CANDIDATE_DELAY_MAX).random().also { tk.logger.info("Delaying for $it ms") })
            log.voteForSelf(tk.port)

            launch {
                val votesReceived = requestVotes()
                tk.logger.info("Received $votesReceived votes")

                if (votesReceived >= (tk.stubs.size + 1.0) / 2.0) {
                    fsm.transition(Event.CandidateReceivesMajority)
                } else {
                    fsm.transition(Event.CandidateTimeout)
                }
            }

            launch {
                delay(CANDIDATE_TIMEOUT)
                fsm.transition(Event.CandidateTimeout)
            }
        }
    }

    private suspend fun becomeLeader(): Job = coroutineScope {
        return@coroutineScope launch {
            tk.logger.info("BecomeLeader")
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

        return results.count { it } + 1 // Always vote for self
    }

    // Return true if vote granted, else false. Perform error checking on returned value
    private suspend fun requestVote(client: Int, info: PeerInfo): Boolean {
        tk.logger.info("Requesting vote from $client")
        val req = log.buildVoteRequest(tk.port)
        val res = info.raftStub.vote(req)

        if (res.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }

        return res.voteGranted
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
        val req = log.buildAppendRequest(tk.port, info.nextIndex, log.leaderCommit())
        val res = info.raftStub.append(req)

        if (res.term > log.term()) {
            fsm.transition(Event.HigherTermServer)
        }

        // TODO: Reconcile log if follower is behind

        return info.copy()
    }
}
