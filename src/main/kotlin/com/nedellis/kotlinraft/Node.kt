package com.nedellis.kotlinraft

import com.tinder.StateMachine

sealed class State {
    object Follower : State()
    object Candidate : State()
    object Leader : State()
}

sealed class Event {
    object FollowerTimeout : Event()
    object CandidateTimeout : Event()
    object CandidateReceivesMajority : Event()
    object LeaderRefreshTimer : Event()
    object HigherTermServer : Event()
}

sealed class SideEffect {
    object HoldElection : SideEffect()
    object RefreshFollowers : SideEffect()
}

class Node {
    private val fsm = StateMachine.create<State, Event, SideEffect> {
        state<State.Follower> {
            on<Event.FollowerTimeout> {
                transitionTo(State.Candidate, SideEffect.HoldElection)
            }
        }
        state<State.Follower> {
            on<Event.CandidateTimeout> {
                transitionTo(State.Candidate, SideEffect.HoldElection)
            }
            on<Event.CandidateReceivesMajority> {
                transitionTo(State.Leader, SideEffect.RefreshFollowers)
            }
            on<Event.HigherTermServer> {
                transitionTo(State.Follower)
            }
        }
        state<State.Follower> {
            on<Event.LeaderRefreshTimer> {
                transitionTo(State.Leader, SideEffect.RefreshFollowers)
            }
            on<Event.HigherTermServer> {
                transitionTo(State.Follower)
            }
        }
        onTransition {
            val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
            when (validTransition.sideEffect) {
                SideEffect.HoldElection -> TODO()
                SideEffect.RefreshFollowers -> TODO()
            }
        }
    }

    suspend fun append(req: AppendRequest): AppendResponse {}
    suspend fun vote(req: VoteRequest): VoteResponse {}
}