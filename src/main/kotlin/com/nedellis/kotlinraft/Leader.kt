package com.nedellis.kotlinraft

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout

private data class FollowerState(var nextIndex: Int = 0, var matchIndex: Int = 0)

class Leader(private val state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
        coroutineScope {
            tk.logger.info("Starting leader")

            val followers = tk.raftStubs.keys.map {
                it to FollowerState()
            }.toMap()

            val ticker = Channel<Unit>()
            launch {
                delay(1000L)
                ticker.send(Unit)
            }
            val responses = Channel<AppendResponse>()

            while (true) {
                select<Unit> {
                    ticker.onReceive {
                        for (stub in tk.raftStubs.values) {
                            launch {
//                                tk.logger.info("Sending AppendRequest to $stub")
                                val req = AppendRequest.newBuilder()
                                    .setTerm(state.currentTerm)
                                    .build()
                                // TODO: Add other fields
                                withTimeout(1000L) { responses.send(stub.append(req)) }
                            }
                        }

                        launch {
                            delay(1000L)
                            ticker.send(Unit)
                        }
                    }
                    responses.onReceive {
                        it.convertIfTermHigher(state, outChan)
//                            TODO("If term is ok")
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
                            is Rpc.UpdateEntry -> {
                                state.log.add(it.req)
                                it.replyWithStatus(UpdateStatus.Status.OK)
                            }
                            is Rpc.GetEntry -> {
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

    private fun buildAppendRequest(state: State, followerState: FollowerState): AppendRequest {
        val delta = state.log.slice(followerState.matchIndex..state.log.size)

        val req = AppendRequest.newBuilder()
//            .addAllEntries(delta)

        return req.build()
    }
}
