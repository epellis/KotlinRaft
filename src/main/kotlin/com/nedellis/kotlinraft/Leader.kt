//package com.nedellis.kotlinraft
//
//import kotlinx.coroutines.channels.Channel
//import kotlinx.coroutines.channels.ReceiveChannel
//import kotlinx.coroutines.channels.SendChannel
//import kotlinx.coroutines.coroutineScope
//import kotlinx.coroutines.delay
//import kotlinx.coroutines.launch
//import kotlinx.coroutines.selects.select
//import kotlinx.coroutines.withTimeout
//
//private data class FollowerInfo(var nextIndex: Int, var matchIndex: Int, val info: PeerInfo, val port: Int)
//private data class FollowerResponse(val res: AppendResponse, val info: FollowerInfo)
//
//class Leader(private val state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
//    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
//        coroutineScope {
//            tk.logger.info("Starting leader")
//
//            val followers = (tk.stubs).map { (port, info) ->
//                FollowerInfo(state.log.size + 1, 0, info, port)
//            }
//
//            val ticker = Channel<Unit>()
//            launch {
//                ticker.send(Unit)
//                delay(1000L)
//            }
//            val responses = Channel<FollowerResponse>()
//
//            while (true) {
//                select<Unit> {
//                    ticker.onReceive {
//                        for (info in followers) {
//                            launch {
//                                val req = buildAppendRequest(tk.port, state, info)
//                                val res = withTimeout(1000L) { info.info.raftStub.append(req) }
//                                responses.send(FollowerResponse(res, info))
//                            }
//                        }
//
//                        launch {
//                            delay(1000L)
//                            ticker.send(Unit)
//                        }
//                    }
//                    responses.onReceive {
//                        it.res.convertIfTermHigher(state, outChan)
//
//                        if (it.res.success) {
//                            it.info.nextIndex = state.log.size + 1
//                            it.info.matchIndex = state.log.size
//                        } else {
//                            tk.logger.warn("Failed to update ${it.info.port} next index to ${it.info.nextIndex}")
//                            it.info.nextIndex--
//                            val req = buildAppendRequest(tk.port, state, it.info)
//                            tk.logger.info("ReSending update to ${it.info.port}")
//                            val res = withTimeout(1000L) { it.info.info.raftStub.append(req) }
//                            responses.send(FollowerResponse(res, it.info))
//                        }
//                    }
//                    inChan.onReceive {
//                        when (it) {
//                            is Rpc.AppendEntries -> {
//                                it.convertIfTermHigher(state, outChan)
//                                if (it.denyIfTermLower(state)) {
//                                    return@onReceive
//                                }
//                            }
//                            is Rpc.RequestVote -> {
//                                it.vote(Role.LEADER, state, outChan)
//                            }
//                            is Rpc.UpdateEntry -> {
//                                state.log.add(it.req)
//
//                                // TODO: Broadcast update to followers and wait until half respond before replying success
//
//                                it.replyWithStatus(UpdateStatus.Status.OK)
//                            }
//                            is Rpc.GetEntry -> {
//                                val entry = state.find(it.req)
//                                if (entry !== null) {
//                                    it.replyWithStatus(GetStatus.Status.OK, entry)
//                                } else {
//                                    it.replyWithStatus(GetStatus.Status.NOT_FOUND)
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
//
//    private fun buildAppendRequest(port: Int, state: State, info: FollowerInfo): AppendRequest {
//        val delta = state.log.subList(info.matchIndex, state.log.size).toList()
//
//        val req = AppendRequest.newBuilder()
//            .setTerm(state.currentTerm)
//            .setLeaderID(port)
//            .setPrevLogIndex(info.matchIndex) // TODO: could be wrong
//            .setPrevLogTerm(state.currentTerm) // TODO: could be wrong
//            .addAllEntries(delta)
//            .setLeaderCommit(state.log.size)
//
//        return req.build()
//    }
//}
