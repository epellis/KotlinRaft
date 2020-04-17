package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class Raft(private val port: Int, private val clients: List<Int>) {
    private val stubs = mutableMapOf<Int, RaftGrpcKt.RaftCoroutineStub>()

    init {
        for (client in clients) {
            if (client != port) {
                val channel = ManagedChannelBuilder.forAddress("localhost", client)
                    .usePlaintext()
                    .executor(Dispatchers.Default.asExecutor())
                    .build()
                stubs[client] = RaftGrpcKt.RaftCoroutineStub(channel)
            }
        }
    }

    @ObsoleteCoroutinesApi
    suspend fun run() = coroutineScope {
        val raftStateActor = raftStateActor()
        Thread {
            ServerBuilder.forPort(port)
                .addService(RaftService(raftStateActor))
                .addService(ControllerService(raftStateActor))
                .build()
                .start()
                .awaitTermination()
        }.start()
    }

    // Manages Raft State Machine
    // Routes messages to individual actors
    // Handles state change between transitions
    @ObsoleteCoroutinesApi
    private fun CoroutineScope.raftStateActor() = actor<RaftStateActorMessage> {
        var votingState = VotingState()
        val logState = LogState()

        var role = RaftRole.FOLLOWER
        var roleJob = Job()
        var roleActor: SendChannel<Any> = followerActor(logState, roleJob, channel) as SendChannel<Any>

        for (msg in channel) {
            when (msg) {
                is RaftStateActorMessage.AppendEntries -> {
                    if (msg.req.term > votingState.currentTerm) {
                        channel.send(RaftStateActorMessage.ConvertToFollower)
                        channel.send(RaftStateActorMessage.UpdateTerm(msg.req.term))
                        channel.send(msg) // Resend message so that we can deal with it once term is updated
                    } else if (role == RaftRole.FOLLOWER) {
                        roleActor.send(FollowerActorMessage.AppendEntries(msg.req, msg.res))
                    } else {
                        val response = AppendEntriesResponse.newBuilder()
                            .setTerm(votingState.currentTerm)
                            .setSuccess(false)
                            .build()
                        msg.res.complete(response)
                    }
                }
                is RaftStateActorMessage.RequestVote -> {
                    if (msg.req.term > votingState.currentTerm) {
                        channel.send(RaftStateActorMessage.ConvertToFollower)
                        channel.send(RaftStateActorMessage.UpdateTerm(msg.req.term))
                        channel.send(msg) // Resend message so that we can deal with it once term is updated
                    } else if (votingState.votedFor == null) {
                        channel.send(RaftStateActorMessage.UpdateVote(msg.req.candidateID))
                        val response = RequestVoteResponse.newBuilder()
                            .setTerm(votingState.currentTerm)
                            .setVoteGranted(true)
                            .build()
                        msg.res.complete(response)
                    } else {
                        val response = RequestVoteResponse.newBuilder()
                            .setTerm(votingState.currentTerm)
                            .setVoteGranted(false)
                            .build()
                        msg.res.complete(response)
                    }
                }
                is RaftStateActorMessage.UpdateTerm -> {
                    votingState = VotingState(msg.currentTerm, null)
                }
                is RaftStateActorMessage.UpdateVote -> {
                    votingState = VotingState(votingState.currentTerm, msg.id)
                }
                is RaftStateActorMessage.ConvertToLeader -> {
                    role = RaftRole.LEADER
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = leaderActor(logState, roleJob, channel) as SendChannel<Any>
                }
                is RaftStateActorMessage.ConvertToCandidate -> {
                    role = RaftRole.CANDIDATE
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = candidateActor(votingState, roleJob, channel) as SendChannel<Any>
                }
                is RaftStateActorMessage.ConvertToFollower -> {
                    role = RaftRole.FOLLOWER
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = followerActor(logState, roleJob, channel) as SendChannel<Any>
                }
            }
        }
    }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.leaderActor(logState: LogState, job: Job, parent: SendChannel<RaftStateActorMessage>) =
        actor<LeaderActorMessage>(context = job) {
            // Periodically update followers
            launch {
                while (true) {
                    channel.send(LeaderActorMessage.UpdateFollowers)
                    delay(1000L)
                }
            }

            for (msg in channel) {
                when (msg) {
                    is LeaderActorMessage.AppendEntriesResult -> {

                    }
                    is LeaderActorMessage.UpdateFollowers -> {

                    }
                }
            }
        }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.candidateActor(
        votingState: VotingState,
        job: Job,
        parent: SendChannel<RaftStateActorMessage>
    ) =
        actor<CandidateActorMessage>(context = job) {
            // In 1 second, timeout voting
            launch {
                delay(1000L)
                channel.send(CandidateActorMessage.VotingTimeout)
            }

            var votes = 0

            for (client in clients) {
                launch {
                    val request = RequestVoteRequest.newBuilder()
                        .setCandidateID(port)
                        .setTerm(votingState.currentTerm)
                        .build()
                    // TODO: Set other terms
                    val response = stubs[client]?.requestVote(request)
                    if (response != null) {
                        channel.send(CandidateActorMessage.RequestVotesResult(response))
                    }
                }
            }

            for (msg in channel) {
                when (msg) {
                    is CandidateActorMessage.VotingTimeout -> {
                        when {
                            votes > clients.size / 2.0 -> parent.send(RaftStateActorMessage.ConvertToLeader)
                            else -> parent.send(RaftStateActorMessage.ConvertToCandidate)
                        }
                    }
                    is CandidateActorMessage.RequestVotesResult -> {
                        if (msg.res.term > votingState.currentTerm) {
                            parent.send(RaftStateActorMessage.UpdateTerm(msg.res.term))
                        } else if (msg.res.voteGranted) {
                            votes++
                        }
                    }
                }
            }
        }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.followerActor(
        logState: LogState,
        job: Job,
        parent: SendChannel<RaftStateActorMessage>
    ) =
        actor<FollowerActorMessage>(context = job) {
            // In 3 seconds, timeout leader if not canceled
            var timeoutJob = Job()
            launch(context = job) {
                delay(3000L)
                channel.send(FollowerActorMessage.LeaderTimeout)
            }

            for (msg in channel) {
                when (msg) {
                    is FollowerActorMessage.LeaderTimeout -> {
                        parent.send(RaftStateActorMessage.ConvertToCandidate)
                    }
                    is FollowerActorMessage.AppendEntries -> {
                        // Reset timeout
                        timeoutJob.cancel()
                        timeoutJob = Job()
                        launch(context = job) {
                            delay(3000L)
                            channel.send(FollowerActorMessage.LeaderTimeout)
                        }

                        // Update Internal State TODO

                        // Reply Back TODO
                    }
                }
            }
        }

    sealed class RaftStateActorMessage {
        data class AppendEntries(val req: AppendEntriesRequest, val res: CompletableDeferred<AppendEntriesResponse>) :
            RaftStateActorMessage()

        data class RequestVote(val req: RequestVoteRequest, val res: CompletableDeferred<RequestVoteResponse>) :
            RaftStateActorMessage()

        data class UpdateTerm(val currentTerm: Int) : RaftStateActorMessage()
        data class UpdateVote(val id: Int) : RaftStateActorMessage()

        object ConvertToFollower : RaftStateActorMessage()
        object ConvertToCandidate : RaftStateActorMessage()
        object ConvertToLeader : RaftStateActorMessage()
    }

    sealed class LeaderActorMessage {
        object UpdateFollowers : LeaderActorMessage()
        data class AppendEntriesResult(val res: RequestVoteResponse) : LeaderActorMessage()
    }

    sealed class CandidateActorMessage {
        object VotingTimeout : CandidateActorMessage()
        data class RequestVotesResult(val res: RequestVoteResponse) : CandidateActorMessage()
    }

    sealed class FollowerActorMessage {
        object LeaderTimeout : FollowerActorMessage()
        data class AppendEntries(val req: AppendEntriesRequest, val res: CompletableDeferred<AppendEntriesResponse>) :
            FollowerActorMessage()
    }

    internal data class VotingState(
        val currentTerm: Int = 1,
        val votedFor: Int? = null
    )

    internal data class LogState(
        val log: List<Int> = listOf(),
        val commitIndex: Int = 1,
        val lastApplied: Int = 1
    )

    internal data class ServerState(
        val nextIndex: Map<Int, Int>,
        val matchIndex: Map<Int, Int>
    )

    internal enum class RaftRole {
        LEADER, CANDIDATE, FOLLOWER
    }

    internal class RaftService(val actor: SendChannel<RaftStateActorMessage>) : RaftGrpcKt.RaftCoroutineImplBase() {
        override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
            val res = CompletableDeferred<AppendEntriesResponse>()
            actor.send(RaftStateActorMessage.AppendEntries(request, res))
            return res.await()
        }

        override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
            val res = CompletableDeferred<RequestVoteResponse>()
            actor.send(RaftStateActorMessage.RequestVote(request, res))
            return res.await()
        }
    }

    internal class ControllerService(val actor: SendChannel<RaftStateActorMessage>) :
        RaftControllerGrpcKt.RaftControllerCoroutineImplBase() {
        override suspend fun getEntry(request: Key): Entry {
            return super.getEntry(request)
        }

        override suspend fun removeEntry(request: Key): Status {
            return super.removeEntry(request)
        }

        override suspend fun setEntry(request: Entry): Status {
            return super.setEntry(request)
        }
    }
}
