package com.nedellis.kotlinraft

import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select


class Leader(private val state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
    @ObsoleteCoroutinesApi
    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
        coroutineScope {
            tk.logger.info("Starting leader")

            val ticker = ticker(1000L)
            val responses = Channel<AppendResponse>()

            while (true) {
                select<Unit> {
                    ticker.onReceive {
                        for ((client, stub) in tk.raftStubs) {
                            if (client != tk.port) {
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
                            is Rpc.SetEntry -> {
                                tk.logger.info("Processing set unavailable")
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