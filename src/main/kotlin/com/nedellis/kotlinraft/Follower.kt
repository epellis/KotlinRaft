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
                                tk.logger.info("AppendEntries")
                                it.convertIfTermHigher(state, outChan)
                                if (it.denyIfTermLower(state)) {
                                    return@onReceive
                                }

                                // TODO: Update log
                                if (it.req.entriesList.size > 0) {
                                    tk.logger.info("Received a nonzero amount of entries to update")
                                }

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
                            is Rpc.UpdateEntry -> {
                                val leaderStub = tk.stubs[state.votedFor]?.controlStub
                                if (leaderStub == null) {
                                    it.replyWithStatus(UpdateStatus.Status.UNAVAILABLE)
                                } else {
                                    it.forwardToLeader(leaderStub)
                                }
                            }
                            is Rpc.GetEntry -> {
                                val leaderStub = tk.stubs[state.votedFor]?.controlStub
                                if (leaderStub == null) {
                                    it.replyWithStatus(GetStatus.Status.UNAVAILABLE)
                                } else {
                                    val entry = state.find(it.req)
                                    if (entry !== null) {
                                        it.replyWithStatus(GetStatus.Status.OK, entry)
                                    } else {
                                        it.replyWithStatus(GetStatus.Status.NOT_FOUND)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
}
