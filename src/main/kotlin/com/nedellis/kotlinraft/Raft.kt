package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min

class Raft(val port: Int, val clients: List<Int>) {
    @ObsoleteCoroutinesApi
    suspend fun run() = coroutineScope {
        val raftStateActor = raftStateActor()
        val server = ServerBuilder.forPort(port)
            .addService(RaftService(raftStateActor))
            .addService(ControllerService(raftStateActor))
            .build()
        Thread { server.start().awaitTermination() }
    }

    // Manages Raft State Machine
    // Routes messages to individual actors
    // Handles state change between transitions
    @ObsoleteCoroutinesApi
    private fun CoroutineScope.raftStateActor() = actor<RaftStateActorMessage> {
        val votingState = VotingState()
        val logState = LogState()

        var roleJob = Job()
        var roleActor: SendChannel<Any> = followerActor(logState, roleJob) as SendChannel<Any>

        for (msg in channel) {
            when (msg) {
                is RaftStateActorMessage.AppendEntries -> {
                    if (msg.req.term > votingState.currentTerm) {
                    }
                }
                is RaftStateActorMessage.RequestVote -> {
                    // TODO
                }
                is RaftStateActorMessage.ConvertToLeader -> {
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = leaderActor(logState, roleJob) as SendChannel<Any>
                }
                is RaftStateActorMessage.ConvertToCandidate -> {
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = candidateActor(roleJob) as SendChannel<Any>
                }
                is RaftStateActorMessage.ConvertToFollower -> {
                    roleJob.cancel()
                    roleJob = Job()
                    roleActor = followerActor(logState, roleJob) as SendChannel<Any>
                }
            }
        }
    }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.leaderActor(logState: LogState, job: Job) =
        actor<LeaderActorMessage>(context = job) {
            for (msg in channel) {

            }
        }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.candidateActor(job: Job) =
        actor<CandidateActorMessage>(context = job) {
            // In 1 second, timeout voting
            launch {
                delay(1000L)
                channel.send(CandidateActorMessage.VotingTimeout)
            }

            for (msg in channel) {

            }
        }

    @ObsoleteCoroutinesApi
    private fun CoroutineScope.followerActor(logState: LogState, job: Job) =
        actor<FollowerActorMessage>(context = job) {
            for (msg in channel) {

            }
        }

    sealed class RaftStateActorMessage {
        data class AppendEntries(val req: AppendEntriesRequest, val res: CompletableDeferred<AppendEntriesResponse>) :
            RaftStateActorMessage()

        data class RequestVote(val req: RequestVoteRequest, val res: CompletableDeferred<RequestVoteResponse>) :
            RaftStateActorMessage()

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
