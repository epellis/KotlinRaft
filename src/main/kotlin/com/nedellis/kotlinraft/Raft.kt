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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class Raft(val port: Int, val clients: List<Int>) {
    private val requestVoteChannel = Channel<RaftAction.RequestVote>(Channel.UNLIMITED)
    private val appendEntryChannel = Channel<RaftAction.AppendEntries>(Channel.UNLIMITED)
    private val electionResultsChannel = Channel<RaftAction.ElectionResults>(Channel.UNLIMITED)
    private val setEntryChannel = Channel<ControlAction.SetEntry>(Channel.UNLIMITED)
    private val removeEntryChannel = Channel<ControlAction.RemoveEntry>(Channel.UNLIMITED)
    private val getEntryChannel = Channel<ControlAction.GetEntry>(Channel.UNLIMITED)
    private val server = ServerBuilder.forPort(port)
        .addService(RaftImpl(requestVoteChannel, appendEntryChannel))
        .build()
    private val controller = ServerBuilder.forPort(port)
        .addService(ControllerImpl(setEntryChannel, removeEntryChannel, getEntryChannel))
        .build()
    private val raftStubs = mutableMapOf<Int, RaftGrpc.RaftFutureStub>()
    private val state = RaftState()
    private val logger = LoggerFactory.getLogger("Raft $port")
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

    suspend fun run() = coroutineScope {
        logger.info("Running on port: $port")
        Thread { server.start().awaitTermination() }.start()
        Thread { controller.start().awaitTermination() }.start()

        val electionTimeoutTicker = ticker(3000L)
        val leaderPingTicker = ticker(1000L)
        val leaderTimeoutChannel = Channel<Unit>()

        var leaderTimeoutCoroutine = launch { checkForLeaderTimeout(leaderTimeoutChannel) }

        // Event loop, reads from action channels
        while (true) {
            select<Unit> {
                requestVoteChannel.onReceive { message ->
                    requestVote(message)
                }
                appendEntryChannel.onReceive { message ->
                    appendEntries(message)
                    leaderTimeoutCoroutine.cancel()
                    leaderTimeoutCoroutine = launch { checkForLeaderTimeout(leaderTimeoutChannel) }
                }
                leaderTimeoutChannel.onReceive {
                    if (role == RaftRole.FOLLOWER) {
                        logger.info("Transition to candidate")
                        role = RaftRole.CANDIDATE
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
                        if (results.votesReceived >= clients.size / 2.0) {
                            launch {
                                updateFollowers()
                            }
                            role = RaftRole.LEADER
                        }
                    }
                }
            }
        }
    }

    private fun appendEntries(ctx: RaftAction.AppendEntries) {
        if (ctx.req.leaderID != port) {
            role = RaftRole.FOLLOWER
        }

        // Reply false if term < currentTerm
        if (ctx.req.term < state.currentTerm) {
            val reply = AppendEntriesResponse.newBuilder().setSuccess(false).setTerm(state.currentTerm).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
            return
        }

        // Reply false if log doesn't contain an entry at prevLogindex whose term matches prevLogTerm
        if (ctx.req.prevLogIndex > state.log.size || ctx.req.prevLogTerm != state.currentTerm) {
            val reply = AppendEntriesResponse.newBuilder().setSuccess(false).setTerm(state.currentTerm).build()
            ctx.res.onNext(reply)
            ctx.res.onCompleted()
            return
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
        val reply = RequestVoteResponse.newBuilder().setTerm(state.currentTerm)

        // If RPC term is higher than current term, update current term and convert to follower
        if (ctx.req.term > state.currentTerm) {
            state.currentTerm = ctx.req.term
            role = RaftRole.FOLLOWER
        }

        // Reply false if term < currentTerm
        if (ctx.req.term < state.currentTerm) {
            logger.info("Denying ${ctx.req} because term is stale")
            ctx.res.onNext(reply.setVoteGranted(false).build())
            ctx.res.onCompleted()
            return
        }

        // Reply false if client has already voted
        if (state.lastTermVoted == ctx.req.term && state.votedFor != ctx.req.candidateID) {
            logger.info("Denying ${ctx.req} because already voted for ${state.votedFor}")
            ctx.res.onNext(reply.setVoteGranted(false).build())
            ctx.res.onCompleted()
            return
        }

        state.lastTermVoted = ctx.req.term
        state.votedFor = ctx.req.candidateID
        logger.info("Voting for ${ctx.req.candidateID}")
        ctx.res.onNext(reply.setVoteGranted(true).build())
        ctx.res.onCompleted()
    }

    private suspend fun checkForLeaderTimeout(channel: Channel<Unit>) {
        delay(3000L)
        channel.send(Unit)
    }

    private suspend fun startNewElection() {
        logger.info("Starting new election")
        state.currentTerm++
        state.votedFor = port

        val term = state.currentTerm

        val votesReceived = AtomicInteger()

        withContext(Dispatchers.IO) {
            for (client in clients) {
                launch {
                    val request = RequestVoteRequest.newBuilder()
                        .setCandidateID(port)
                        .setLastLogIndex(state.log.size)
                        .setLastLogTerm(state.currentTerm) // TODO: This is probably wrong
                        .setTerm(state.currentTerm)
                        .build()
                    raftStubs[client]?.withDeadlineAfter(500, TimeUnit.MILLISECONDS)?.let { stub ->
                        try {
                            val response = stub.requestVote(request).await()
                            if (response.term > state.currentTerm) {
                                state.currentTerm = term
                                // TODO: Should we do anything more than this?
                            } else if (response.voteGranted) {
                                votesReceived.incrementAndGet()
                            }
                        } catch (e: io.grpc.StatusRuntimeException) {
                            logger.info("Election request to to $client timed out")
                        }
                    }
                }
            }
        }

        electionResultsChannel.send(RaftAction.ElectionResults(votesReceived.get(), term))
    }

    private suspend fun updateFollowers() {
        coroutineScope {
            for (client in clients) {
                launch {
                    val request = AppendEntriesRequest.newBuilder()
                        .setTerm(state.currentTerm)
                        .setLeaderID(port)
                        .setPrevLogIndex(0) // TODO
                        .setPrevLogTerm(state.currentTerm) // TODO
                        .setLeaderCommit(0) // TODO
                        .build()
                    raftStubs[client]?.withDeadlineAfter(500, TimeUnit.MILLISECONDS)?.let { stub ->
                        try {
                            val response = stub.appendEntries(request).await()
                            // TODO: update metadata based on response
                        } catch (e: io.grpc.StatusRuntimeException) {
                            logger.info("AppendEntries request to $client failed")
                        }
                    }
                }
            }
        }
    }

    // Schema for handoff between threaded GRPC server and suspending select functions
    internal sealed class RaftAction {
        data class AppendEntries(val req: AppendEntriesRequest, val res: StreamObserver<AppendEntriesResponse>) :
            RaftAction()

        data class RequestVote(val req: RequestVoteRequest, val res: StreamObserver<RequestVoteResponse>) :
            RaftAction()

        data class ElectionResults(val votesReceived: Int, val term: Int) : RaftAction()
    }

    // Schema for the handoff between threaded controller server and suspending select functions
    internal sealed class ControlAction {
        data class SetEntry(val req: Entry, val res: StreamObserver<Status>) : ControlAction()
        data class RemoveEntry(val req: Key, val res: StreamObserver<Status>) : ControlAction()
        data class GetEntry(val req: Key, val res: StreamObserver<Entry>) : ControlAction()
    }

    // Enumerate all 3 states that the raft client can be in
    internal enum class RaftRole {
        LEADER, CANDIDATE, FOLLOWER
    }

    // All raft overhead data necessary
    internal data class RaftState(
        var currentTerm: Int = 0,
        var votedFor: Int? = null,
        var lastTermVoted: Int = 0,
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

    // Runs the externally facing control interface
    internal class ControllerImpl(
        val setEntryChannel: Channel<ControlAction.SetEntry>,
        val removeEntryChannel: Channel<ControlAction.RemoveEntry>,
        val getEntryChannel: Channel<ControlAction.GetEntry>
    ) : RaftControllerGrpc.RaftControllerImplBase() {
        override fun setEntry(request: Entry?, responseObserver: StreamObserver<Status>?) {
            setEntryChannel.offer(ControlAction.SetEntry(request!!, responseObserver!!))
        }

        override fun removeEntry(request: Key?, responseObserver: StreamObserver<Status>?) {
            removeEntryChannel.offer(ControlAction.RemoveEntry(request!!, responseObserver!!))
        }

        override fun getEntry(request: Key?, responseObserver: StreamObserver<Entry>?) {
            getEntryChannel.offer(ControlAction.GetEntry(request!!, responseObserver!!))
        }
    }
}
