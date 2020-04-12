package com.nedellis.kotlinraft

import io.grpc.ManagedChannelBuilder
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.launch

class Raft(val port: Int, val discoveryPort: Int) {
    private val discoveryChannel = ManagedChannelBuilder.forAddress("localhost", discoveryPort)
        .usePlaintext()
        .build()
    private val discoveryStub = DiscoveryGrpc.newFutureStub(discoveryChannel)
    private var clients = mutableSetOf<Int>()
    private val messageChannel = Channel<RaftMessage>(Channel.UNLIMITED)
    private val server = ServerBuilder.forPort(port)
        .addService(RaftImpl(messageChannel))
        .build()
    private val state = RaftState()
    val table = mutableMapOf<String, String>()

    private suspend fun registerAndGetClients(): MutableSet<Int> {
        val request = RegisterNewClientRequest.newBuilder().setPort(port).build()
        val response = discoveryStub.registerNewClient(request).await()
        return response.clientPortsList.toMutableSet()
    }

    fun run() = GlobalScope.launch {
        clients = registerAndGetClients()

        Thread { server.start().awaitTermination() }.start()

        // Event loop coroutine, reads from GRPC received message channel
        launch {
            while (true) {
                val message = messageChannel.receive()
                when (message) {
                    is RaftMessage.AppendEntries -> appendEntries(message)
                    is RaftMessage.RequestVote -> requestVote(message)
                }
            }
        }
    }

    private fun appendEntries(ctx: RaftMessage.AppendEntries) {

    }

    private fun requestVote(ctx: RaftMessage.RequestVote) {

    }

    internal sealed class RaftMessage {
        data class AppendEntries(val req: AppendEntriesRequest, val res: StreamObserver<AppendEntriesResponse>) :
            RaftMessage()

        data class RequestVote(val req: RequestVoteRequest, val res: StreamObserver<RequestVoteResponse>) :
            RaftMessage()
    }

    internal data class RaftState(
        var currentTerm: Int = 0,
        var votedFor: Int = 0,
        var log: List<Int> = emptyList(),
        var commitIndex: Int = 0,
        var lastApplied: Int = 0,
        var nextIndex: MutableMap<Int, Int> = mutableMapOf(),
        var matchIndex: MutableMap<Int, Int> = mutableMapOf()
    )

    // Runs the GRPC server. When a message is received, add to message channel and return immediately
    internal class RaftImpl(val messageChannel: Channel<RaftMessage>) : RaftGrpc.RaftImplBase() {
        override fun appendEntries(
            request: AppendEntriesRequest?,
            responseObserver: StreamObserver<AppendEntriesResponse>?
        ) {
            messageChannel.offer(RaftMessage.AppendEntries(request!!, responseObserver!!))
        }

        override fun requestVote(request: RequestVoteRequest?, responseObserver: StreamObserver<RequestVoteResponse>?) {
            messageChannel.offer(RaftMessage.RequestVote(request!!, responseObserver!!))
        }
    }
}
