package com.nedellis.kotlinraft

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel

const val FOLLOWER_TIMEOUT = 5000L
const val CANDIDATE_DELAY_MAX = 100L
const val CANDIDATE_TIMEOUT = 1000L
const val LEADER_TIMEOUT = 1000L

@ExperimentalCoroutinesApi
class Node(private val tk: Toolkit) {
    private var ctx: Job? = null
    private val changeStateChan = Channel<SideEffect>(Channel.CONFLATED) // Last Write Wins
    private val fsm = FSM(changeStateChan)
    private val log = Log(logger = tk.logger)

    suspend fun run() = coroutineScope {
        // Event loop listens for next state change event
        launch {
            while (true) {
                val effect = changeStateChan.receive()
                ctx?.cancel()
                ctx = launch {
                    when (effect) {
                        SideEffect.BecomeFollower -> becomeFollower()
                        SideEffect.BecomeCandidate -> becomeCandidate()
                        SideEffect.BecomeLeader -> becomeLeader()
                    }
                }
            }
        }

        // Kick start as follower
        changeStateChan.send(SideEffect.BecomeFollower)
    }

    suspend fun append(req: AppendRequest): AppendResponse {
//        tk.logger.info("Append Request: $req, LOG: $log")
        if (req.term > log.term()) {
            log.changeTerm(req.term)
            fsm.transition(Event.HigherTermServer)
        }
        if (req.term == log.term() && fsm.state == State.Candidate) {
            fsm.transition(Event.HigherTermServer)
        }
        if (fsm.state == State.Follower) {
            fsm.transition(Event.FollowerUpdated)
        }
        return log.append(req)
    }

    suspend fun vote(req: VoteRequest): VoteResponse {
        if (req.term > log.term()) {
            log.changeTerm(req.term)
            fsm.transition(Event.HigherTermServer)
        }
        return log.vote(req)
    }

    suspend fun getEntry(req: Key): GetStatus {
        tk.logger.info("Searching for $req")
        return log.get(req)
    }

    suspend fun updateEntry(req: Entry): UpdateStatus {
        tk.logger.info("Updating state machine with $req")

        val statusUnavailable = UpdateStatus.newBuilder().setStatus(UpdateStatus.Status.UNAVAILABLE).build()

        return when (fsm.state) {
            State.Leader -> log.update(req)
            State.Candidate -> statusUnavailable
            State.Follower -> {
                val stub = log.leader()?.let {
                    tk.stubs[it]?.controlStub
                }
                tk.logger.info("Passing request to leader: ${log.leader()}, $stub")
                stub?.updateEntry(req) ?: statusUnavailable
            }
        }
    }

    private suspend fun becomeFollower() = coroutineScope {
        tk.logger.info("BecomeFollower")
        delay(FOLLOWER_TIMEOUT)
        tk.logger.info("Follower Timed Out")
        fsm.transition(Event.FollowerTimeout)
    }

    private suspend fun becomeCandidate() = coroutineScope {
        tk.logger.info("BecomeCandidate")
        delay((0L..CANDIDATE_DELAY_MAX).random().also { tk.logger.info("Delaying for $it ms") })
        log.changeTerm(log.term() + 1)
        log.voteForSelf(tk.port)

        launch {
            val majority = (tk.stubs.size + 1.0) / 2.0
            val electionWon = requestVotes(majority)

            if (electionWon) {
                fsm.transition(Event.CandidateReceivesMajority)
            } else {
                fsm.transition(Event.CandidateTimeout)
            }
        }

        launch {
            delay(CANDIDATE_TIMEOUT)
            tk.logger.info("CandidateTimeout")
            fsm.transition(Event.CandidateTimeout)
        }
    }

    private suspend fun becomeLeader() = coroutineScope {
        tk.logger.info("BecomeLeader")
        launch {
            refreshFollowers()
        }

        launch {
            delay(LEADER_TIMEOUT)
            fsm.transition(Event.LeaderRefreshTimer)
        }
    }

    private suspend fun requestVotes(majority: Double): Boolean = coroutineScope {
        val count = AtomicInteger(1) // Always vote for self
        val resultsChan = Channel<Boolean>()

        val job = launch {
            tk.stubs.map { (client, info) ->
                async {
                    val result = requestVote(client, info)
                    if (result) {
                        val currentVotes = count.incrementAndGet()
                        if (currentVotes >= majority) {
                            resultsChan.send(true)
                        }
                    }
                }
            }.awaitAll()

            resultsChan.send(false)
        }

        return@coroutineScope resultsChan.receive().also { job.cancel() }
    }

    // Return true if vote granted, else false. Perform error checking on returned value
    private suspend fun requestVote(client: Int, info: PeerInfo): Boolean {
        tk.logger.info("Requesting vote from $client")
        val req = log.buildVoteRequest(tk.port)
        val res = info.raftStub.vote(req)

        if (res.term > log.term()) {
            tk.logger.info("Vote: $client has term ${res.term} my term is ${log.term()}")
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
            tk.logger.info("Refresh: $client has term ${res.term} my term is ${log.term()}")
            fsm.transition(Event.HigherTermServer)
        }

        // TODO: Reconcile log if follower is behind

        return info.copy()
    }
}
