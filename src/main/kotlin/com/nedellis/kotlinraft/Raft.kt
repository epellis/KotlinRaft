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
            var actor = launch { Follower(State(id = port)).run(actorChan, stateChangeChan) }

            while (true) {
                select<Unit> {
                    inChan.onReceive {
                        actorChan.send(it)
                    }
                    stateChangeChan.onReceive {
                        logger.info("Canceling actor with state: ${it.state}, switch to ${it.role}")
                        actor.cancel()
                        actor = when (it.role) {
                            Role.LEADER -> launch {
                                Leader(it.state).run(actorChan, stateChangeChan)
                            }
                            Role.CANDIDATE -> launch {
                                Candidate(it.state).run(actorChan, stateChangeChan)
                            }
                            Role.FOLLOWER -> launch {
                                Follower(it.state).run(actorChan, stateChangeChan)
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

    private inner class Leader(private val state: State) : IOActor<Rpc, ChangeRole> {
        @ObsoleteCoroutinesApi
        override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
            coroutineScope {
                logger.info("Starting leader")

                val ticker = ticker(1000L)
                val responses = Channel<AppendResponse>()

                while (true) {
                    select<Unit> {
                        ticker.onReceive {
                            for ((client, stub) in raftStubs) {
                                if (client != port) {
                                    launch {
//                                        logger.info("Sending AppendRequest to $client")
                                        val req = AppendRequest.newBuilder()
                                            .setTerm(state.currentTerm)
                                            .build()
                                        // TODO: Add other fields
                                        responses.send(stub.append(req))
                                    }
                                }
                            }
                        }
                        responses.onReceive {
                            it.convertIfTermHigher(state, outChan)
                            TODO("If term is ok")
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    it.convertIfTermHigher(state, outChan)
                                    if (it.denyIfTermLower(state)) {
                                        return@onReceive
                                    }
                                }
                                is Rpc.RequestVote -> {
                                    it.vote(Role.LEADER, state, outChan)
                                }
                                is Rpc.SetEntry -> {
                                    logger.info("Processing set unavailable")
                                    it.replyUnavailable()
                                }
                                is Rpc.RemoveEntry -> {
                                    it.replyUnavailable()
                                }
                                is Rpc.GetEntry -> {
                                    it.replyUnavailable()
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

                val timeout = Channel<Unit>()
                val responses = Channel<VoteResponse>()

                launch {
                    delay((1000..3000).random().toLong()) // Delay a random amount before timing out
                    timeout.send(Unit)
                }

                for ((client, stub) in raftStubs) {
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
                            val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = state.id)
                            outChan.send(ChangeRole(Role.CANDIDATE, nextState, null))
                        }
                        responses.onReceive {
                            logger.info("Vote Response: ${it.voteGranted}")
                            it.convertIfTermHigher(state, outChan)
                            if (it.voteGranted) {
                                votesGranted++
                            }
                            logger.info("Now have $votesGranted votes in term ${state.currentTerm}")
                            if (votesGranted > raftStubs.size / 2.0) {
                                outChan.send(ChangeRole(Role.LEADER, state, null))
                            }
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    it.convertIfTermHigher(state, outChan)
                                    if (it.denyIfTermLower(state)) {
                                        return@onReceive
                                    }

                                    outChan.send(ChangeRole(Role.FOLLOWER, state, null))
                                }
                                is Rpc.RequestVote -> {
                                    it.vote(Role.CANDIDATE, state, outChan)
                                }
                                is Rpc.SetEntry -> {
                                    it.replyUnavailable()
                                }
                                is Rpc.RemoveEntry -> {
                                    it.replyUnavailable()
                                }
                                is Rpc.GetEntry -> {
                                    it.replyUnavailable()
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

                val timeoutChan = Channel<Unit>()
                var timeoutJob = launch {
                    delay(3000L)
                    timeoutChan.send(Unit)
                }

                while (true) {
                    select<Unit> {
                        timeoutChan.onReceive {
                            logger.info("Follower Timed Out")
                            val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = state.id)
                            outChan.send(ChangeRole(Role.CANDIDATE, nextState, null))
                        }
                        inChan.onReceive {
                            when (it) {
                                is Rpc.AppendEntries -> {
                                    it.convertIfTermHigher(state, outChan)
                                    if (it.denyIfTermLower(state)) {
                                        return@onReceive
                                    }

                                    // TODO: Update log

                                    timeoutJob.cancel()
                                    timeoutJob = launch {
                                        delay(3000L)
                                        timeoutChan.send(Unit)
                                    }
                                }
                                is Rpc.RequestVote -> {
                                    it.vote(Role.FOLLOWER, state, outChan)
                                }
                                is Rpc.SetEntry -> {
                                    val leaderStub = controlStubs[state.votedFor]
                                    if (leaderStub == null) {
                                        it.replyUnavailable()
                                    } else {
                                        it.forwardToLeader(leaderStub)
                                    }
                                }
                                is Rpc.RemoveEntry -> {
                                    val leaderStub = controlStubs[state.votedFor]
                                    if (leaderStub == null) {
                                        it.replyUnavailable()
                                    } else {
                                        it.forwardToLeader(leaderStub)
                                    }
                                }
                                is Rpc.GetEntry -> {
                                    val leaderStub = controlStubs[state.votedFor]
                                    if (leaderStub == null) {
                                        it.replyUnavailable()
                                    } else {
                                        it.forwardToLeader(leaderStub)
                                    }
                                }
                            }
                        }
                    }
                }
            }
    }

    private data class State(
        val id: Int = 1,
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
            logger.info("AppendResponse has term of $term I have term of ${state.currentTerm}")
            val nextState = state.copy(currentTerm = term, votedFor = null)
            supervisorChan.send(ChangeRole(Role.FOLLOWER, nextState, null))
        }
    }

    private suspend fun VoteResponse.convertIfTermHigher(state: State, supervisorChan: SendChannel<ChangeRole>) {
        if (term > state.currentTerm) {
            logger.info("VoteResponse has term of $term I have term of ${state.currentTerm}")
            val nextState = state.copy(currentTerm = term, votedFor = null)
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
}
