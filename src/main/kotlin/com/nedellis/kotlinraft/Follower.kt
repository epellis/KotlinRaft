package com.nedellis.kotlinraft

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select


class Follower(private var state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
        coroutineScope {
            tk.logger.info("Starting follower")

            val timeoutChan = Channel<Unit>()
            var timeoutJob = launch {
                delay(3000L)
                timeoutChan.send(Unit)
            }

            while (true) {
                select<Unit> {
                    timeoutChan.onReceive {
                        tk.logger.info("Follower Timed Out")
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

                                val response = AppendResponse.newBuilder()
                                    .setSuccess(true)
                                    .setTerm(state.currentTerm)
                                    .build()
                                it.res.complete(response)

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
                                val leaderStub = tk.controlStubs[state.votedFor]
                                if (leaderStub == null) {
                                    it.replyUnavailable()
                                } else {
                                    it.forwardToLeader(leaderStub)
                                }
                            }
                            is Rpc.RemoveEntry -> {
                                val leaderStub = tk.controlStubs[state.votedFor]
                                if (leaderStub == null) {
                                    it.replyUnavailable()
                                } else {
                                    it.forwardToLeader(leaderStub)
                                }
                            }
                            is Rpc.GetEntry -> {
                                val leaderStub = tk.controlStubs[state.votedFor]
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
