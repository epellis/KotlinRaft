package com.nedellis.kotlinraft

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout

class Candidate(private val state: State, private val tk: Toolkit) : IOActor<Rpc, ChangeRole> {
    override suspend fun run(inChan: ReceiveChannel<Rpc>, outChan: SendChannel<ChangeRole>) =
        coroutineScope {
            tk.logger.info("Starting candidate")

            val timeout = Channel<Unit>()
            val responses = Channel<VoteResponse>()

            launch {
                val delayPeriod = (1000..3000).random().toLong()
                tk.logger.info("Waiting $delayPeriod ms for others to vote")
                delay(delayPeriod) // Delay a random amount before timing out
                timeout.send(Unit)
            }

            for (info in tk.stubs.values) {
                launch {
                    val req = VoteRequest.newBuilder()
                        .setCandidateID(tk.port)
                        .setLastLogIndex(state.log.size) // TODO
                        .setLastLogTerm(state.currentTerm) // TODO
                        .setTerm(state.currentTerm)
                        .build()
                    withTimeout(1000L) { responses.send(info.raftStub.vote(req)) }
                }
            }

            var votesGranted = 1 // Candidates should always vote for themselves

            while (true) {
                select<Unit> {
                    timeout.onReceive {
                        tk.logger.info("Voting Timeout")
                        val nextState = state.copy(currentTerm = state.currentTerm + 1, votedFor = state.id)
                        if (tk.stubs.isEmpty() || votesGranted > (1 + tk.stubs.size) / 2.0) {
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
                        if (votesGranted > (1 + tk.stubs.size) / 2.0) {
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
                            is Rpc.UpdateEntry -> {
                                it.replyWithStatus(UpdateStatus.Status.UNAVAILABLE)
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
