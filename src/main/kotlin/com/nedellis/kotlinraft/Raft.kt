package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.selects.select
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class Raft(private val port: Int, private val clients: List<Int>) {
    private val raftStubs = clients.filter { it != port }.map { it ->
        it to RaftGrpcKt.RaftCoroutineStub(
            ManagedChannelBuilder.forAddress("localhost", it)
                .usePlaintext()
                .executor(Dispatchers.IO.asExecutor())
                .build()
        )
    }.toMap()

    private val controlStubs = clients.filter { it != port }.map { it ->
        it to ControlGrpcKt.ControlCoroutineStub(
            ManagedChannelBuilder.forAddress("localhost", it)
                .usePlaintext()
                .executor(Dispatchers.IO.asExecutor())
                .build()
        )
    }.toMap()

    private val logger = LoggerFactory.getLogger("Raft $port")

    suspend fun run() = coroutineScope {
        val gRPCtoCoordinatorChan = Channel<MetaRpc>(Channel.UNLIMITED)

        launch(Dispatchers.IO) {
            ServerBuilder.forPort(port)
                .addService(RaftService(gRPCtoCoordinatorChan, logger))
                .addService(ControlService(gRPCtoCoordinatorChan, logger))
                .executor(Dispatchers.IO.asExecutor())
                .build()
                .start()
                .awaitTermination()
        }

        Coordinator().run(gRPCtoCoordinatorChan)
    }

    private inner class Coordinator : IActor<MetaRpc> {
        override suspend fun run(inChan: ReceiveChannel<MetaRpc>) = coroutineScope {
            logger.info("Starting coordinator")

            val actorChan = Channel<Rpc>() // Synchronous
            val stateChangeChan = Channel<ChangeRole>() // Synchronous
            val tk = Toolkit(logger, port, raftStubs, controlStubs)
            var actor = launch { Follower(State(id = port), tk).run(actorChan, stateChangeChan) }

            while (true) {
                select<Unit> {
                    inChan.onReceive {
                        when (it) {
                            is MetaRpc.Idle -> if (actor.isActive) {
                                logger.info("Idle")
                                actor.cancel()
                            }
                            is MetaRpc.Wake -> if (actor.isCancelled) {
                                logger.info("Wake")
                                actor = launch { Follower(State(id = port), tk).run(actorChan, stateChangeChan) }
                            }
                            is MetaRpc.RpcWrapper -> actorChan.send(it.rpc)
                        }
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
    val commitIndex: Int = 1,
    val lastApplied: Int = 1,
    private val log: MutableList<LogEntry> = mutableListOf()
) {
    sealed class LogEntry {
        data class Addition(val entry: Entry) : LogEntry()
        data class Deletion(val key: Key) : LogEntry()
    }

    fun add(entry: Entry) {
        log.add(LogEntry.Addition(entry))
    }

    fun delete(key: Key) {
        log.add(LogEntry.Deletion(key))
    }

    fun find(key: Key): Entry? {
        for (item in log.reversed()) {
            when (item) {
                is LogEntry.Addition -> if (item.entry.key == key.key) return item.entry
                is LogEntry.Deletion -> if (item.key.key == key.key) return null
            }
        }

        return null
    }
}

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
        val res: CompletableDeferred<AppendResponse>,
        val logger: Logger? = null
    ) : Rpc(), TermChecked {
        override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
            if (req.term > state.currentTerm) {
                logger?.info("Changing State to FOLLOWER")
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
        val res: CompletableDeferred<VoteResponse>,
        val logger: Logger? = null
    ) : Rpc(), TermChecked {
        override suspend fun convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
            if (req.term > state.currentTerm) {
                logger?.info("Changing State to FOLLOWER")
                val nextState = state.copy(currentTerm = req.term, votedFor = null)
                supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, this))
            }
        }

        suspend fun vote(currentRole: Role, state: State, supervisorChan: SendChannel<ChangeRole>) {
            let {
                val response = VoteResponse.newBuilder()
                    .setTerm(state.currentTerm)
                    .setVoteGranted(false)
                    .build()

                logger?.info("Processing vote for ${req.candidateID}, term: ${req.term}, my state: $state")

                // Reply false if term < currentTerm
                if (req.term < state.currentTerm) {
                    logger?.info("Denying vote because their term of ${req.term} is less than my term of ${state.currentTerm}")
                    res.complete(response)
                    return
                }

                // Reply false if already voted for another candidate this term
                if (state.currentTerm == req.term && state.votedFor != null && state.votedFor != req.candidateID) {
                    logger?.info("Denying vote because already voted for ${state.votedFor}")
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

                logger?.info("Term ${state.currentTerm} voting for: ${req.candidateID}")
                res.complete(response)

                val nextState = state.copy(votedFor = req.candidateID, currentTerm = req.term)
                supervisorChan.send(ChangeRole(currentRole, nextState, null))
            }
        }
    }

    data class SetEntry(val req: Entry, val res: CompletableDeferred<SetStatus>, val logger: Logger? = null) : Rpc() {
        fun replyWithStatus(status: SetStatus.Status) {
            val response = SetStatus.newBuilder().setStatus(status).build()
            res.complete(response)
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            logger?.info("Forwarding $req to leader")
            res.complete(stub.setEntry(req))
        }
    }

    data class RemoveEntry(val req: Key, val res: CompletableDeferred<RemoveStatus>, val logger: Logger? = null) :
        Rpc() {
        fun replyWithStatus(status: RemoveStatus.Status) {
            val response = RemoveStatus.newBuilder().setStatus(status).build()
            res.complete(response)
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            res.complete(stub.removeEntry(req))
        }
    }

    data class GetEntry(val req: Key, val res: CompletableDeferred<GetStatus>, val logger: Logger? = null) : Rpc() {
        fun replyWithStatus(status: GetStatus.Status, entry: Entry? = null) {
            val response = GetStatus.newBuilder().setStatus(status)
            if (entry !== null) {
                response.setEntry(entry)
            }
            res.complete(response.build())
        }

        suspend fun forwardToLeader(stub: ControlGrpcKt.ControlCoroutineStub) {
            res.complete(stub.getEntry(req))
        }
    }
}

sealed class MetaRpc {
    object Idle : MetaRpc()
    object Wake : MetaRpc()
    data class RpcWrapper(val rpc: Rpc) : MetaRpc() // Pass through from coordinator to raft role actor
}

data class ChangeRole(val role: Role, val state: State, val msg: Rpc?)

enum class Role {
    LEADER, CANDIDATE, FOLLOWER
}

private class RaftService(private val actor: SendChannel<MetaRpc>, private val logger: Logger? = null) :
    RaftGrpcKt.RaftCoroutineImplBase() {
    override suspend fun append(request: AppendRequest): AppendResponse {
        val res = CompletableDeferred<AppendResponse>()
        actor.send(MetaRpc.RpcWrapper(Rpc.AppendEntries(request, res, logger)))
        return withTimeout(1000L) { res.await() }
    }

    override suspend fun vote(request: VoteRequest): VoteResponse {
        val res = CompletableDeferred<VoteResponse>()
        actor.send(MetaRpc.RpcWrapper(Rpc.RequestVote(request, res, logger)))
        return withTimeout(1000L) { res.await() }
    }
}

private class ControlService(
    private val actor: SendChannel<MetaRpc>,
    private val logger: Logger? = null
) :
    ControlGrpcKt.ControlCoroutineImplBase() {
    override suspend fun getEntry(request: Key): GetStatus {
        val res = CompletableDeferred<GetStatus>()
        actor.send(MetaRpc.RpcWrapper(Rpc.GetEntry(request, res)))
        return res.await()
    }

    override suspend fun removeEntry(request: Key): RemoveStatus {
        val res = CompletableDeferred<RemoveStatus>()
        actor.send(MetaRpc.RpcWrapper(Rpc.RemoveEntry(request, res)))
        return res.await()
    }

    override suspend fun setEntry(request: Entry): SetStatus {
        val res = CompletableDeferred<SetStatus>()
        actor.send(MetaRpc.RpcWrapper(Rpc.SetEntry(request, res, logger)))
        return res.await()
    }

    override suspend fun idleClient(request: Nothing): Nothing {
        actor.send(MetaRpc.Idle)
        return Nothing.newBuilder().build()
    }

    override suspend fun wakeClient(request: Nothing): Nothing {
        actor.send(MetaRpc.Wake)
        return Nothing.newBuilder().build()
    }
}
