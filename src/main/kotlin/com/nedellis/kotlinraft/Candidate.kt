package com.nedellis.kotlinraft

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select

class Candidate(private val state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
        coroutineScope {
            tk.logger.info("Starting candidate")

            val timeout = Channel<Unit>()
            val responses = Channel<VoteResponse>()

            launch {
                delay((1000..3000).random().toLong()) // Delay a random amount before timing out
                timeout.send(Unit)
            }

            for (stub in tk.raftStubs.values) {
                launch {
                    val req = VoteRequest.newBuilder()
                        .setCandidateID(tk.port)
                        .setLastLogIndex(0) // TODO
                        .setLastLogTerm(0) // TODO
                        .setTerm(state.currentTerm)
                        .build()
                    responses.send(stub.vote(req))
                }
            }

            var votesGranted = 1 // Candidates should always vote for themselves

            while (true) {
                select<Unit> {
                    timeout.onReceive {
                        val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = state.id)
                        if (tk.raftStubs.isEmpty() || votesGranted > (1 + tk.raftStubs.size) / 2.0) {
                            outChan.send(ChangeRole(Role.LEADER, state, null))
                        }
                        outChan.send(ChangeRole(Role.CANDIDATE, nextState, null))
                    }
                    responses.onReceive {
                        tk.logger.info("Vote Response: ${it.voteGranted}")
                        it.convertIfTermHigher(state, outChan)
                        if (it.voteGranted) {
                            votesGranted++
                        }
                        tk.logger.info("Now have $votesGranted votes in term ${state.currentTerm}")
                        if (votesGranted > (1 + tk.raftStubs.size) / 2.0) {
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

                                tk.logger.info("Candidate switching back to follower")
                                outChan.send(ChangeRole(Role.FOLLOWER, state, it))
                            }
                            is Rpc.RequestVote -> {
                                it.vote(Role.CANDIDATE, state, outChan)
                            }
                            is Rpc.SetEntry -> {
                                it.replyWithStatus(SetStatus.Status.UNAVAILABLE)
                            }
                            is Rpc.RemoveEntry -> {
                                it.replyWithStatus(RemoveStatus.Status.UNAVAILABLE)
                            }
                            is Rpc.GetEntry -> {
                                it.replyWithStatus(GetStatus.Status.UNAVAILABLE)
                            }
                        }
                    }
                }
            }
        }
}
