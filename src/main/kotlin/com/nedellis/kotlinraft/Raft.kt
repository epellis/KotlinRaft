package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val stubs = mutableMapOf<Int, RaftGrpcKt.RaftCoroutineStub>()
    private val logger = LoggerFactory.getLogger("Raft $port")

    init {
        for (client in clients) {
            val channel = ManagedChannelBuilder.forAddress("localhost", client)
                .usePlaintext()
                .executor(Dispatchers.Default.asExecutor())
                .build()
            stubs[client] = RaftGrpcKt.RaftCoroutineStub(channel)
        }
    }

    suspend fun run() = coroutineScope {
        val gRPCtoCoordinatorChan = Channel<Rpc>(Channel.UNLIMITED)

        Thread {
            ServerBuilder.forPort(port)
                .addService(RaftService(gRPCtoCoordinatorChan))
                .build()
                .start()
                .awaitTermination()
        }.start()

        launch {
            val coordinator = Coordinator().run(gRPCtoCoordinatorChan)
        }
    }

    private inner class Coordinator : IActor<Rpc> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>) = coroutineScope {
            logger.info("Starting coordinator")

            val actorChan = Channel<Rpc>(Channel.UNLIMITED) // Asynchronous
            val stateChangeChan = Channel<StateChange>() // Synchronous
            var actor = launch { Follower(State()).run(actorChan, stateChangeChan) }

            while (true) {
                logger.info("LOOP")
                select<Unit> {
                    inChan.onReceive {
                        logger.info("Dispatching Message: $it")
                        actorChan.send(it)
                    }
                    stateChangeChan.onReceive {
                        logger.info("Canceling actor with state: $it")
                        actor.cancel()
                        actor = when (it) {
                            is StateChange.ChangeToLeader -> launch {
                                Leader(it.state).run(actorChan, stateChangeChan)
                            }
                            is StateChange.ChangeToCandidate -> launch {
                                Candidate(it.state).run(actorChan, stateChangeChan)
                            }
                            is StateChange.ChangeToFollower -> launch {
                                Follower(it.state).run(actorChan, stateChangeChan)
                            }
                        }
                    }
                }
            }
        }
    }

    private inner class Leader(private val state: State) : IOActor<Rpc, StateChange> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<StateChange>) =
            coroutineScope {
                logger.info("Starting leader")

                while (true) {
                    select<Unit> {
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    if (it.req.term > state.currentTerm) {
                                        val res = AppendEntriesResponse.newBuilder()
                                            .setSuccess(false)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                        state.votedFor = null
                                        state.currentTerm = it.req.term
                                        outChan.send(StateChange.ChangeToFollower(state))
                                    } else {
                                        val res = AppendEntriesResponse.newBuilder()
                                            .setSuccess(false)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                    }
                                }
                                is Rpc.RequestVote -> {
                                    // If candidate has higher term, convert to follower and grant vote
                                    if (it.req.term > state.currentTerm) {
                                        state.currentTerm = it.req.term
                                        state.votedFor = it.req.candidateID
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    private inner class Candidate(private val state: State) : IOActor<Rpc, StateChange> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<StateChange>) =
            coroutineScope {
                logger.info("Starting candidate")

                val timeout = ticker(1000L)
                val responses = Channel<RequestVoteResponse>()

                state.currentTerm++
                state.votedFor = port;

                for ((client, stub) in stubs) {
                    if (client != port) {
                        launch {
                            val req = RequestVoteRequest.newBuilder()
                                .setCandidateID(port)
                                .setLastLogIndex(0) // TODO
                                .setLastLogTerm(0) // TODO
                                .setTerm(state.currentTerm)
                                .build()
                            responses.send(stub.requestVote(req))
                        }
                    }
                }

                var votesGranted = 1

                while (true) {
                    select<Unit> {
                        timeout.onReceive {
                            if (votesGranted > stubs.size / 2.0) {
                                outChan.send(StateChange.ChangeToLeader(state))
                            } else {
                                outChan.send(StateChange.ChangeToCandidate(state))
                            }
                        }
                        responses.onReceive {
                            if (it.term > state.currentTerm) {
                                outChan.send(StateChange.ChangeToFollower(state.copy(currentTerm = it.term)))
                            }
                            if (it.voteGranted) {
                                votesGranted++
                            }
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    if (it.req.term > state.currentTerm) {
                                        val res = AppendEntriesResponse.newBuilder()
                                            .setSuccess(false)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                        state.votedFor = null
                                        state.currentTerm = it.req.term
                                        outChan.send(StateChange.ChangeToFollower(state))
                                    }
                                }
                                is Rpc.RequestVote -> {
                                    if (it.req.term > state.currentTerm) {
                                        val res = RequestVoteResponse.newBuilder()
                                            .setVoteGranted(true)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                        state.currentTerm = it.req.term
                                        state.votedFor = it.req.candidateID
                                        outChan.send(StateChange.ChangeToFollower(state))
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    private inner class Follower(private var state: State) : IOActor<Rpc, StateChange> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<StateChange>) =
            coroutineScope {
                logger.info("Starting follower")

                val ticker = ticker(3000L)
                var timedOut = true

                while (true) {
                    select<Unit> {
                        ticker.onReceive {
                            logger.info("Follower Timed Out")
                            if (timedOut) {
                                outChan.send(StateChange.ChangeToCandidate(state))
                            } else {
                                timedOut = true
                            }
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    if (it.req.term > state.currentTerm) {
                                        state = state.copy(currentTerm = it.req.term, votedFor = null)
                                    }
                                    timedOut = false
                                }
                                is Rpc.RequestVote -> {
                                    if (it.req.term > state.currentTerm) {
                                        state.currentTerm = it.req.term
                                        state.votedFor = null
                                    }

                                    // Reply false if term < currentTerm
                                    if (it.req.term < state.currentTerm) {
                                        val res = RequestVoteResponse.newBuilder()
                                            .setVoteGranted(false)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                    }

                                    // If votedFor is null or candidateID and candidates log is at least as up to date as receivers log, grant vote
                                    if (state.votedFor == null || state.votedFor == it.req.candidateID) {
                                        state.votedFor = it.req.candidateID
                                        val res = RequestVoteResponse.newBuilder()
                                            .setVoteGranted(true)
                                            .setTerm(state.currentTerm)
                                            .build()
                                        it.res.complete(res)
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    private suspend fun convertIfHigherTerm(msg: Rpc, state: State, outChan: SendChannel<StateChange>): State {
        when (msg) {
            is Rpc.AppendEntries -> {
            }
            is Rpc.RequestVote -> {
            is Rpc.RequestVote -> {
            }
        }
    }

    internal data class State(
        val currentTerm: Int = 1,
        val votedFor: Int? = null,
        val log: List<Int> = listOf(),
        val commitIndex: Int = 1,
        val lastApplied: Int = 1
    )

    internal sealed class Rpc {
        internal data class AppendEntries(
            val req: AppendEntriesRequest,
            val res: CompletableDeferred<AppendEntriesResponse>
        ) : Rpc()

        internal data class RequestVote(
            val req: RequestVoteRequest,
            val res: CompletableDeferred<RequestVoteResponse>
        ) :
            Rpc()
    }

    internal sealed class StateChange {
        data class ChangeToLeader(val state: State) : StateChange()
        data class ChangeToCandidate(val state: State) : StateChange()
        data class ChangeToFollower(val state: State) : StateChange()
    }

    internal class RaftService(val actor: SendChannel<Rpc>) : RaftGrpcKt.RaftCoroutineImplBase() {
        override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
            val res = CompletableDeferred<AppendEntriesResponse>()
            actor.send(Rpc.AppendEntries(request, res))
            return res.await()
        }

        override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
            val res = CompletableDeferred<RequestVoteResponse>()
            actor.send(Rpc.RequestVote(request, res))
            return res.await()
        }
    }
}
