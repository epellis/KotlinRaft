package com.nedellis.kotlinraft

import java.lang.IllegalArgumentException
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

const val FOLLOWER_TIMEOUT = 5000L
const val CANDIDATE_DELAY_MAX = 100L
const val CANDIDATE_TIMEOUT = 1000L
const val LEADER_TIMEOUT = 1000L

// TODO: Consider using scope and lambdas and returning lock on time
private class LogSynchronizer(id: Int) {
    var log = Log(id)
    val mutex = Mutex()

    suspend fun get(): Log = mutex.withLock { return log }
    suspend fun set(newLog: Log) = mutex.withLock { log = newLog }
}

@ExperimentalCoroutinesApi
class Node(private val tk: Toolkit) {
    private var ctx: Job? = null
    private val changeStateChan = Channel<SideEffect>(Channel.CONFLATED) // Last Write Wins
    private val fsm = FSM(changeStateChan)
    private var logSync = LogSynchronizer(tk.port)

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

    @ExperimentalStdlibApi
    suspend fun append(req: AppendRequest): AppendResponse {
        val log = logSync.get()
        tk.logger.info("Append Request: $req, LOG: $log")
        if (req.term > log.term) {
            log.changeTerm(req.term)
            fsm.transition(Event.HigherTermServer)
        }
        if (req.term == log.term && fsm.state == State.Candidate) {
            fsm.transition(Event.HigherTermServer)
        }
        if (fsm.state == State.Follower) {
            fsm.transition(Event.FollowerUpdated)
        }
        val (newLog, res) = log.append(req)
        logSync.set(newLog)
        return res
    }

    suspend fun vote(req: VoteRequest): VoteResponse {
        val log = logSync.get()
        if (req.term > log.term) {
            log.changeTerm(req.term)
            fsm.transition(Event.HigherTermServer)
        }
        val (newLog, res) = log.vote(req)
        logSync.set(newLog)
        tk.logger.info("Vote request from ${req.candidateID} was ${res.voteGranted}")
        return res
    }

    suspend fun getEntry(req: Key): GetStatus {
        tk.logger.info("Searching for $req")
        return logSync.get().get(req)
    }

    @ExperimentalStdlibApi
    suspend fun updateEntry(req: Entry): UpdateStatus {
        val log = logSync.get()
        tk.logger.info("Updating state machine with $req")

        val statusUnavailable = UpdateStatus.newBuilder().setStatus(UpdateStatus.Status.UNAVAILABLE).build()

        return when (fsm.state) {
            State.Leader -> {
                val (newLog, res) = log.update(req)
                logSync.set(newLog)
                res
            }
            State.Candidate -> statusUnavailable
            State.Follower -> {
                val stub = log.votedFor?.let {
                    tk.stubs[it]?.controlStub
                }
                tk.logger.info("Passing request to leader: ${log.votedFor}, $stub")
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
        val log = logSync.get()
        logSync.set(log.changeTerm(log.term + 1))
        logSync.set(logSync.get().voteForSelf())

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
        val log = logSync.get()
        val req = log.buildVoteRequest()
        val res = info.raftStub.vote(req)

        if (res.term > log.term) {
            tk.logger.info("Vote: $client has term ${res.term} my term is ${log.term}")
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
        tk.logger.info("Refreshing follower $client, info: $info")
        val log = logSync.get()

        if (info.nextIndex < 0) {
            throw IllegalArgumentException("Next Index must be non negative")
        }

        val req = log.buildAppendRequest(info.nextIndex, 0)
        val res = info.raftStub.append(req)

        if (res.term > log.term) {
            tk.logger.info("Refresh: $client has term ${res.term} my term is ${log.term}")
            fsm.transition(Event.HigherTermServer)
        }

        return if (res.success) {
            info.copy(nextIndex = log.log.size, matchIndex = info.nextIndex)
        } else {
            val newInfo = info.copy(nextIndex = info.nextIndex - 1)
            refreshFollower(client, newInfo)
        }
    }
}
