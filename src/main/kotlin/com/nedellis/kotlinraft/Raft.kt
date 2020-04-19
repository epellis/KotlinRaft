package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
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
            val stateChangeChan = Channel<ChangeRole>() // Synchronous
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
//                        actor = when (it.role) {
//                            Role.LEADER -> launch {
//                                Leader(it.state).run(actorChan, stateChangeChan)
//                            }
//                            Role.CANDIDATE -> launch {
//                                Candidate(it.state).run(actorChan, stateChangeChan)
//                            }
//                            Role.FOLLOWER -> launch {
//                                Follower(it.state).run(actorChan, stateChangeChan)
//                            }
//                        }
//                        if (it.msg != null) {
//                            actorChan.send(it.msg)
//                        }
                    }
                }
            }
        }
    }

    private inner class Leader(private val state: State) : IOActor<Rpc, ChangeRole> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
            coroutineScope {
                logger.info("Starting leader")

                while (true) {
                    select<Unit> {
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                }
                                is Rpc.RequestVote -> {
                                }
                            }
                        }
                    }
                }
            }
    }

    private inner class Candidate(private val state: State) : IOActor<Rpc, ChangeRole> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
            coroutineScope {
                logger.info("Starting candidate")

                val timeout = ticker(1000L)
                val responses = Channel<VoteResponse>()

                for ((client, stub) in stubs) {
                    if (client != port) {
                        launch {
                            val req = VoteRequest.newBuilder()
                                .setCandidateID(port)
                                .setLastLogIndex(0) // TODO
                                .setLastLogTerm(0) // TODO
                                .setTerm(state.currentTerm)
                                .build()
                            responses.send(stub.vote(req))
                        }
                    }
                }

                var votesGranted = 1 // Candidates should always vote for themselves

                while (true) {
                    select<Unit> {
                        timeout.onReceive {
                            if (votesGranted > stubs.size / 2.0) {
                                outChan.send(ChangeRole(Role.LEADER, state, null))
                            } else {
                                val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = port)
                                outChan.send(ChangeRole(Role.CANDIDATE, nextState, null))
                            }
                        }
                        responses.onReceive {
                            it.convertIfTermHigher(state, outChan)
                            if (it.voteGranted) {
                                votesGranted++
                            }
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    it.convertIfTermHigher(state, outChan)
                                }
                                is Rpc.RequestVote -> {
                                    it.convertIfTermHigher(state, outChan)
                                    it.vote(Role.CANDIDATE, state, outChan)
                                }
                            }
                        }
                    }
                }
            }
    }

    private inner class Follower(private var state: State) : IOActor<Rpc, ChangeRole> {
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
            coroutineScope {
                logger.info("Starting follower")

                val ticker = ticker(3000L)
                var timedOut = true

                while (true) {
                    select<Unit> {
                        ticker.onReceive {
                            logger.info("Follower Timed Out")
                            if (timedOut) {
                                val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = port)
                                outChan.send(ChangeRole(Role.CANDIDATE, nextState, null))
                            } else {
                                timedOut = true
                            }
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    it.convertIfTermHigher(state, outChan)

                                    // TODO: Update log

                                    timedOut = false
                                }
                                is Rpc.RequestVote -> {
                                    it.convertIfTermHigher(state, outChan)
                                    it.vote(Role.FOLLOWER, state, outChan)
                                }
                            }
                        }
                    }
                }
            }
    }

    private data class State(
        val currentTerm: Int = 1,
        val votedFor: Int? = null,
        val log: List<Int> = listOf(),
        val commitIndex: Int = 1,
        val lastApplied: Int = 1
    )

    private interface TermChecked {
        suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>)
    }

    private suspend fun AppendResponse.convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
        if (term > state.currentTerm) {
            val nextState = state.copy(currentTerm = term)
            supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, null))
        }
    }

    private suspend fun VoteResponse.convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
        if (term > state.currentTerm) {
            val nextState = state.copy(currentTerm = term)
            supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, null))
        }
    }

    private sealed class Rpc {
        data class AppendEntries(
            val req: AppendRequest,
            val res: CompletableDeferred<AppendResponse>
        ) : Rpc(), TermChecked {
            override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
                if (req.term > state.currentTerm) {
                    val nextState = state.copy(currentTerm = req.term)
                    supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, this))
                }
            }
        }

        data class RequestVote(
            val req: VoteRequest,
            val res: CompletableDeferred<VoteResponse>
        ) : Rpc(), TermChecked {
            override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
                if (req.term > state.currentTerm) {
                    val nextState = state.copy(currentTerm = req.term)
                    supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, this))
                }
            }

            suspend fun vote(currentRole: Role, state: State, supervisorChan: SendChannel<ChangeRole>) {
                val response = VoteResponse.newBuilder().setTerm(state.currentTerm)

                // Reply false if term < currentTerm
                if (req.term < state.currentTerm) {
                    response.voteGranted = false
                    res.complete(response.build())
                    return
                }

                // Reply false if already voted for another candidate this term
                if (state.votedFor != null && state.votedFor != req.term) {
                    response.voteGranted = false
                    res.complete(response.build())
                }

                // TODO: Check Log

                response.voteGranted = true
                res.complete(response.build())

                val nextState = state.copy(votedFor = req.candidateID)
                supervisorChan.send(ChangeRole(currentRole, nextState, null))
            }
        }
    }

    private data class ChangeRole(val role: Role, val state: State, val msg: Rpc?)

    private enum class Role {
        LEADER, CANDIDATE, FOLLOWER
    }

    private class RaftService(val actor: SendChannel<Rpc>) : RaftGrpcKt.RaftCoroutineImplBase() {
        override suspend fun append(request: AppendRequest): AppendResponse {
            val res = CompletableDeferred<AppendResponse>()
            actor.send(Rpc.AppendEntries(request, res))
            return res.await()
        }

        override suspend fun vote(request: VoteRequest): VoteResponse {
            val res = CompletableDeferred<VoteResponse>()
            actor.send(Rpc.RequestVote(request, res))
            return res.await()
        }
    }
}
