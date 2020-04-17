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
        val gRPCtoCoordinatorChan = Channel<Action.Rpc>(Channel.UNLIMITED)

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

    internal class Coordinator : IActor<Action.Rpc> {
        override suspend fun run(inChan: ReceiveChannel<Action.Rpc>): Job = coroutineScope {
            return@coroutineScope launch {
                val actorChan = Channel<Action.Rpc>(Channel.UNLIMITED) // Asynchronous
                val stateChangeChan = Channel<StateChange>() // Synchronous
                var actor = Follower(State()).run(actorChan, stateChangeChan)

                while (true) {
                    select<Unit> {
                        inChan.onReceive {}
                        stateChangeChan.onReceive {
                            actor.cancel()
                            actor = when (it) {
                                is StateChange.ChangeToLeader -> Follower(it.state).run(actorChan, stateChangeChan)
                                is StateChange.ChangeToCandidate -> Follower(it.state).run(actorChan, stateChangeChan)
                                is StateChange.ChangeToFollower -> Follower(it.state).run(actorChan, stateChangeChan)
                            }
                        }
                    }
                }
            }
        }
    }

    internal class Follower(private val state: State) : IOActor<Action.Rpc, StateChange> {
        override suspend fun run(inChan: ReceiveChannel<Action.Rpc>, outChan: SendChannel<StateChange>): Job =
            coroutineScope {
                return@coroutineScope launch {
                    val ticker = ticker(3000L)
                    var timedOut = true

                    while (true) {
                        select<Unit> {
                            ticker.onReceive {
                                if (timedOut) {
                                    outChan.send(StateChange.ChangeToCandidate(state))
                                } else {
                                    timedOut = true
                                }
                            }
                            inChan.onReceive {
                                when (it) {
                                    is Action.Rpc.AppendEntries -> {
                                        // TODO: Figure out term stuff
                                        timedOut = false
                                    }
                                    is Action.Rpc.RequestVote -> {
                                        // TODO: Figure out term stuff
                                    }
                                }
                            }
                        }
                    }
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

    internal sealed class Action {
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

    }

    internal sealed class StateChange {
        data class ChangeToLeader(val state: State) : StateChange()
        data class ChangeToCandidate(val state: State) : StateChange()
        data class ChangeToFollower(val state: State) : StateChange()
    }

    internal class RaftService(val actor: SendChannel<Action.Rpc>) : RaftGrpcKt.RaftCoroutineImplBase() {
        override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
            val res = CompletableDeferred<AppendEntriesResponse>()
            actor.send(Action.Rpc.AppendEntries(request, res))
            return res.await()
        }

        override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
            val res = CompletableDeferred<RequestVoteResponse>()
            actor.send(Action.Rpc.RequestVote(request, res))
            return res.await()
        }
    }

//    internal class ControllerService(val actor: SendChannel<RaftStateActorMessage>) :
//        RaftControllerGrpcKt.RaftControllerCoroutineImplBase() {
//        override suspend fun getEntry(request: Key): Entry {
//            return super.getEntry(request)
//        }
//
//        override suspend fun removeEntry(request: Key): Status {
//            return super.removeEntry(request)
//        }
//
//        override suspend fun setEntry(request: Entry): Status {
//            return super.setEntry(request)
//        }
//    }
}

//
//private class RaftManager : Parent<RaftManager.RaftRole, RaftManager.RaftState> {
//    suspend fun run() = coroutineScope {
//        val actor = Actor(::leaderActorRun, this@RaftManager)
//    }
//
//    override fun changeToRole(role: RaftRole, state: RaftState) {
//        TODO("Not yet implemented")
//    }
//
//    internal data class RaftState(val x: Int)
//
//    internal enum class RaftRole {
//        LEADER, CANDIDATE, FOLLOWER
//    }
//}
//
//private sealed class RaftActions {
//    data class AppendEntries(val req: AppendEntriesRequest, val res: CompletableDeferred<AppendEntriesResponse>) :
//        RaftActions()
//
//    data class RequestVote(val req: RequestVoteRequest, val res: CompletableDeferred<RequestVoteResponse>) :
//        RaftActions()
//}
//
//private sealed class LeaderActions : RaftActions() {
//    object UpdateFollowers : LeaderActions()
//}
//
//private sealed class CandidateActions : RaftActions() {
//    object TimeoutElections : CandidateActions()
//}
//
//private suspend fun leaderActorRun(
//    chan: ReceiveChannel<RaftActions>,
//    parent: Parent<RaftManager.RaftRole, RaftManager.RaftState>
//): Unit {
//    val state = RaftManager.RaftState(1)
//
//    for (msg in chan) {
//        when (msg) {
//            is RaftActions.AppendEntries -> {
//            }
//            is RaftActions.RequestVote -> {
//            }
//            is LeaderActions.UpdateFollowers -> {
//            }
//            else -> throw Exception("No action for message: $msg")
//        }
//    }
//}
//
//private suspend fun candidateActorRun(
//    chan: ReceiveChannel<RaftActions>,
//    parent: Parent<RaftManager.RaftRole, RaftManager.RaftState>
//): Unit {
//    val state = RaftManager.RaftState(1)
//
//    for (msg in chan) {
//        when (msg) {
//            is RaftActions.AppendEntries -> {
//            }
//            is RaftActions.RequestVote -> {
//            }
//            is CandidateActions.TimeoutElections -> {
//            }
//            else -> throw Exception("No action for message: $msg")
//        }
//    }
//}
//
//private suspend fun followerActorRun(
//    chan: ReceiveChannel<RaftActions>,
//    parent: Parent<RaftManager.RaftRole, RaftManager.RaftState>
//): Unit {
//    val state = RaftManager.RaftState(1)
//
//    for (msg in chan) {
//        when (msg) {
//            is RaftActions.AppendEntries -> {
//            }
//            is RaftActions.RequestVote -> {
//            }
//            else -> throw Exception("No action for message: $msg")
//        }
//    }
//}
//
