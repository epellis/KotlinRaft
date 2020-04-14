package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import kotlin.math.min

class Raft(val port: Int, val clients: List<Int>) {
    private val requestVoteChannel = Channel<RaftAction.RequestVote>(Channel.UNLIMITED)
    private val appendEntryChannel = Channel<RaftAction.AppendEntries>(Channel.UNLIMITED)
    private val electionResultsChannel = Channel<RaftAction.ElectionResults>(Channel.UNLIMITED)
    private val server = ServerBuilder.forPort(port)
        .addService(RaftImpl(requestVoteChannel, appendEntryChannel))
        .build()
    private val raftStubs = mutableMapOf<Int, RaftGrpc.RaftFutureStub>()
    private val state = RaftState()
    private val logger = LoggerFactory.getLogger("Raft $port")
    private var hasGottenLeaderPing = false
    private var role = RaftRole.FOLLOWER

    init {
        // Setup a gRPC connection to each peer using HTTP channels
        for (clientPort in clients) {
            val channel = ManagedChannelBuilder.forAddress("localhost", clientPort)
                .usePlaintext()
                .build()
            raftStubs[clientPort] = RaftGrpc.newFutureStub(channel)
        }
    }

    suspend fun run() {
        coroutineScope {
            delay((1..100).random().toLong())

            logger.info("Running on port: $port")
            Thread { server.start().awaitTermination() }.start()

            val leaderTimeoutTicker = ticker(1000L)
            val electionTimeoutTicker = ticker(3000L)
            val leaderPingTicker = ticker(1000L)

            // Event loop, reads from action channels
            while (true) {
                select<Unit> {
                    requestVoteChannel.onReceive { message ->
                        logger.info("Got Request Vote Message: $message")
                        requestVote(message)
                    }
                    appendEntryChannel.onReceive { message ->
                        logger.info("Got Append Entry Message: $message")
                        role = RaftRole.CANDIDATE
                        appendEntries(message)
                    }
                    leaderTimeoutTicker.onReceive {
                        if (role == RaftRole.FOLLOWER) {
                            if (!hasGottenLeaderPing) {
                                logger.info("Transition to candidate")
                                role = RaftRole.CANDIDATE
                            }
                        }
                    }
                    electionTimeoutTicker.onReceive {
                        if (role == RaftRole.CANDIDATE) {
                            launch {
                                startNewElection()
                            }
                        }
                    }
                    leaderPingTicker.onReceive {
                        if (role == RaftRole.LEADER) {
                            launch {
                                updateFollowers()
                            }
                        }
                    }
                    electionResultsChannel.onReceive { results ->
                        if (role == RaftRole.CANDIDATE) {
                            if (results.votesReceived >= clients.size / 2) {
                                launch {
                                    updateFollowers()
                                }
                                role == RaftRole.LEADER
                            }
                        }
                    }
                }
            }
        }
    }

    private fun appendEntries(ctx: RaftAction.AppendEntries) {
        // Reply false if term < currentTerm
        if (ctx.req.term < state.currentTerm) {
            val reply = AppendEntriesResponse.newBuilder().setSuccess(false).setTerm(state.currentTerm).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
        }

        // Reply false if log doesn't contain an entry at prevLogindex whose term matches prevLogTerm
        if (ctx.req.prevLogIndex > state.log.size || ctx.req.prevLogTerm != state.currentTerm) {
            val reply = AppendEntriesResponse.newBuilder().setSuccess(false).setTerm(state.currentTerm).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
        }

        // If the existing entry conflicts with a new one, delete the existing entry and all that follow it

        // Append any new entries not already in the log

        if (ctx.req.leaderCommit > state.commitIndex) {
            state.commitIndex = min(ctx.req.leaderCommit, state.log.size)
        }

        val reply = AppendEntriesResponse.newBuilder().setSuccess(true).setTerm(state.currentTerm).build()
        ctx.res.onNext(reply)
        ctx.res.onCompleted()
    }

    private fun requestVote(ctx: RaftAction.RequestVote) {
        // Reply false if term < currentTerm
        if (ctx.req.term < state.currentTerm) {
            val reply = RequestVoteResponse.newBuilder().setTerm(state.currentTerm).setVoteGranted(false).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
        }

        // If votedFor is null or candidateID and candidate's log is at least as up to date as receiver's log, grant vote
        if ((state.votedFor == null || state.votedFor == ctx.req.candidateID) && ctx.req.lastLogIndex >= state.log.size && ctx.req.lastLogTerm >= state.currentTerm) {
            val reply = RequestVoteResponse.newBuilder().setTerm(state.currentTerm).setVoteGranted(true).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
            state.votedFor = ctx.req.candidateID
        } else {
            val reply = RequestVoteResponse.newBuilder().setTerm(state.currentTerm).setVoteGranted(false).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
        }
    }

    private suspend fun startNewElection() {
        logger.info("Starting new election")
        val term = state.currentTerm

        state.currentTerm++
        state.votedFor = port

        val resultsFuture = clients.map {
            val request = RequestVoteRequest.newBuilder()
                .setCandidateID(port)
                .setLastLogIndex(state.log.size)
                .setLastLogTerm(state.currentTerm) // TODO: This is probably wrong
                .setTerm(state.currentTerm)
                .build()
            raftStubs[it]?.requestVote(request)
        }
        val votesReceived = resultsFuture.map {
            it?.await()
        }.count { (it?.voteGranted == true) }

        electionResultsChannel.send(RaftAction.ElectionResults(votesReceived, term))
    }

    private suspend fun updateFollowers() {
        // TODO: Send appendentries rpc to all nodes
    }

    // Schema for handoff between threaded GRPC server and suspending select functions
    internal sealed class RaftAction {
        data class AppendEntries(val req: AppendEntriesRequest, val res: StreamObserver<AppendEntriesResponse>) :
            RaftAction()

        data class RequestVote(val req: RequestVoteRequest, val res: StreamObserver<RequestVoteResponse>) :
            RaftAction()

        data class ElectionResults(val votesReceived: Int, val term: Int) : RaftAction()
    }

    // Enumerate all 3 states that the raft client can be in
    internal enum class RaftRole {
        LEADER, CANDIDATE, FOLLOWER
    }

    // All raft overhead data necessary
    internal data class RaftState(
        var currentTerm: Int = 0,
        var votedFor: Int? = null,
        var log: List<Pair<String, String>> = emptyList(),
        var commitIndex: Int = 0,
        var lastApplied: Int = 0,
        var nextIndex: MutableMap<Int, Int> = mutableMapOf(),
        var matchIndex: MutableMap<Int, Int> = mutableMapOf()
    )

    // Runs the GRPC server. When a message is received, add to message channel and return immediately
    internal class RaftImpl(
        val requestVoteChannel: Channel<RaftAction.RequestVote>,
        val appendEntriesChannel: Channel<RaftAction.AppendEntries>
    ) : RaftGrpc.RaftImplBase() {
        override fun appendEntries(
            request: AppendEntriesRequest?,
            responseObserver: StreamObserver<AppendEntriesResponse>?
        ) {
            appendEntriesChannel.offer(RaftAction.AppendEntries(request!!, responseObserver!!))
        }

        override fun requestVote(request: RequestVoteRequest?, responseObserver: StreamObserver<RequestVoteResponse>?) {
            requestVoteChannel.offer(RaftAction.RequestVote(request!!, responseObserver!!))
        }
    }
}
