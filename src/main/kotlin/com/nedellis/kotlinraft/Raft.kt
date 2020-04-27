package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val raftStubs = clients.map { it ->
        it to RaftGrpcKt.RaftCoroutineStub(
            ManagedChannelBuilder.forAddress("localhost", it)
                .usePlaintext()
                .executor(Dispatchers.IO.asExecutor())
                .build()
        )
    }.toMap()

    private val controlStubs = clients.map { it ->
        it to ControlGrpcKt.ControlCoroutineStub(
            ManagedChannelBuilder.forAddress("localhost", it)
                .usePlaintext()
                .executor(Dispatchers.IO.asExecutor())
                .build()
        )
    }.toMap()

    private val logger = LoggerFactory.getLogger("Raft $port")

    @ObsoleteCoroutinesApi
    suspend fun run() = coroutineScope {
        val gRPCtoCoordinatorChan = Channel<Rpc>(Channel.UNLIMITED)

        Thread {
            ServerBuilder.forPort(port)
                .addService(RaftService(gRPCtoCoordinatorChan))
                .addService(ControlService(gRPCtoCoordinatorChan))
                .build()
                .start()
                .awaitTermination()
        }.start()

        Coordinator().run(gRPCtoCoordinatorChan)
    }

    private inner class Coordinator : IActor<Rpc> {
        @ObsoleteCoroutinesApi
        override suspend fun run(inChan: ReceiveChannel<Rpc>) = coroutineScope {
            logger.info("Starting coordinator")

            val actorChan = Channel<Rpc>(Channel.UNLIMITED) // Asynchronous
            val stateChangeChan = Channel<ChangeRole>() // Synchronous
            val tk = Toolkit(logger, port, raftStubs, controlStubs)
            var actor = launch { Follower(State(id = port), tk).run(actorChan, stateChangeChan) }

            while (true) {
                select<Unit> {
                    inChan.onReceive {
                        actorChan.send(it)
                    }
                    stateChangeChan.onReceive {
                        logger.info("Canceling actor with state: ${it.state}, switch to ${it.role}")
                        if (it.msg != null) {
                            logger.info("Redelivering Message: ${it.msg}")
                        }
                        actor.cancel()
                        actor = when (it.role) {
                            Role.LEADER -> launch {
                                Leader(it.state, tk).run(actorChan, stateChangeChan)
                            }
                            Role.CANDIDATE -> launch {
                                Candidate(it.state, tk).run(actorChan, stateChangeChan)
                            }
                            Role.FOLLOWER -> launch {
                                Follower(it.state, tk).run(actorChan, stateChangeChan)
                            }
                        }
                        if (it.msg != null) {
                            actorChan.send(it.msg)
                        }
                    }
                }
            }
        }
    }

}

data class State(
    val id: Int = 1,
    val currentTerm: Int = 1,
    val votedFor: Int? = null,
    val log: List<Int> = listOf(),
    val commitIndex: Int = 1,
    val lastApplied: Int = 1
)

interface TermChecked {
    suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>)
}

suspend fun AppendResponse.convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
    if (term > state.currentTerm) {
//        logger.info("AppendResponse has term of $term I have term of ${state.currentTerm}")
        val nextState = state.copy(currentTerm = term, votedFor = null)
        supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, null))
    }
}

suspend fun VoteResponse.convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
    if (term > state.currentTerm) {
//        logger.info("VoteResponse has term of $term I have term of ${state.currentTerm}")
        val nextState = state.copy(currentTerm = term, votedFor = null)
        supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, null))
    }
}

sealed class Rpc {
    data class AppendEntries(
        val req: AppendRequest,
        val res: CompletableDeferred<AppendResponse>
    ) : Rpc(), TermChecked {
        override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
            if (req.term > state.currentTerm) {
                val nextState = state.copy(currentTerm = req.term, votedFor = null)
                supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, this))
            }
        }

        suspend fun denyIfTermLower(state: State): Boolean {
            if (req.term < state.currentTerm) {
                val response = AppendResponse.newBuilder()
                    .setSuccess(false)
                    .setTerm(state.currentTerm)
                    .build()
                res.complete(response)
            }

            return (req.term < state.currentTerm)
        }
    }

    data class RequestVote(
        val req: VoteRequest,
        val res: CompletableDeferred<VoteResponse>
    ) : Rpc(), TermChecked {
        override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
            if (req.term > state.currentTerm) {
                val nextState = state.copy(currentTerm = req.term, votedFor = null)
                supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, this))
            }
        }

        suspend fun vote(currentRole: Role, state: State, supervisorChan: SendChannel<ChangeRole>) {
            val logger = LoggerFactory.getLogger("Raft ${state.id}")
            let {
                val response = VoteResponse.newBuilder()
                    .setTerm(state.currentTerm)
                    .setVoteGranted(false)
                    .build()

                logger.info("Processing vote for ${req.candidateID}, term: ${req.term}, my state: $state")

                // Reply false if term < currentTerm
                if (req.term < state.currentTerm) {
                    logger.info("Denying vote because their term of ${req.term} is less than my term of ${state.currentTerm}")
                    res.complete(response)
                    return
                }

                // Reply false if already voted for another candidate this term
                if (state.currentTerm == req.term && state.votedFor != null && state.votedFor != req.candidateID) {
                    logger.info("Denying vote because already voted for ${state.votedFor}")
                    res.complete(response)
                    return
                }

                // TODO: Check Log
            }

            let {
                val response = VoteResponse.newBuilder()
                    .setTerm(state.currentTerm)
                    .setVoteGranted(true)
                    .build()

                logger.info("Term ${state.currentTerm} voting for: ${req.candidateID}")
                res.complete(response)

                val nextState = state.copy(votedFor = req.candidateID, currentTerm = req.term)
                supervisorChan.send(ChangeRole(currentRole, nextState, null))
            }
        }
    }

    data class SetEntry(val req: Entry, val res: CompletableDeferred<SetStatus>) : Rpc() {
        fun replyUnavailable() {
            val response = SetStatus.newBuilder().setStatus(SetStatus.Status.UNAVAILABLE).build()
            res.complete(response)
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            res.complete(stub.setEntry(req))
        }
    }

    data class RemoveEntry(val req: Key, val res: CompletableDeferred<RemoveStatus>) : Rpc() {
        fun replyUnavailable() {
            val response = RemoveStatus.newBuilder().setStatus(RemoveStatus.Status.UNAVAILABLE).build()
            res.complete(response)
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            res.complete(stub.removeEntry(req))
        }
    }

    data class GetEntry(val req: Key, val res: CompletableDeferred<GetStatus>) : Rpc() {
        fun replyUnavailable() {
            val response = GetStatus.newBuilder().setStatus(GetStatus.Status.UNAVAILABLE).build()
            res.complete(response)
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            res.complete(stub.getEntry(req))
        }
    }
}

data class ChangeRole(val role: Role, val state: State, val msg: Rpc?)

enum class Role {
    LEADER, CANDIDATE, FOLLOWER
}

private class RaftService(val actor: SendChannel<Rpc>) : RaftGrpcKt.RaftCoroutineImplBase() {
    override suspend fun append(request: AppendRequest): AppendResponse {
        val res = CompletableDeferred<AppendResponse>()
        actor.send(Rpc.AppendEntries(request, res))
        return withTimeout(1000L) { res.await() }
    }

    override suspend fun vote(request: VoteRequest): VoteResponse {
        val res = CompletableDeferred<VoteResponse>()
        actor.send(Rpc.RequestVote(request, res))
        return withTimeout(1000L) { res.await() }
    }
}

private class ControlService(val actor: SendChannel<Rpc>) : ControlGrpcKt.ControlCoroutineImplBase() {
    override suspend fun getEntry(request: Key): GetStatus {
        val res = CompletableDeferred<GetStatus>()
        actor.send(Rpc.GetEntry(request, res))
        return res.await()
    }

    override suspend fun removeEntry(request: Key): RemoveStatus {
        val res = CompletableDeferred<RemoveStatus>()
        actor.send(Rpc.RemoveEntry(request, res))
        return res.await()
    }

    override suspend fun setEntry(request: Entry): SetStatus {
        val res = CompletableDeferred<SetStatus>()
        actor.send(Rpc.SetEntry(request, res))
        return res.await()
    }
}
