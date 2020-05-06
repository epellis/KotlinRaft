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
    object BecomeFollower : SideEffect()
    object BecomeCandidate : SideEffect()
    object BecomeLeader : SideEffect()
}

object FSM {
    operator fun invoke(
        changeState: (SideEffect) -> Boolean
    ): StateMachine<State, Event, SideEffect> {
        return StateMachine.create<State, Event, SideEffect> {
            initialState(State.Follower)
            state<State.Follower> {
                on<Event.FollowerTimeout> {
                    transitionTo(State.Candidate, SideEffect.BecomeCandidate)
                }
            }
            state<State.Candidate> {
                on<Event.CandidateTimeout> {
                    transitionTo(State.Candidate, SideEffect.BecomeCandidate)
                }
                on<Event.CandidateReceivesMajority> {
                    transitionTo(State.Leader, SideEffect.BecomeLeader)
                }
                on<Event.HigherTermServer> {
                    transitionTo(State.Follower, SideEffect.BecomeFollower)
                }
            }
            state<State.Leader> {
                on<Event.LeaderRefreshTimer> {
                    transitionTo(State.Leader, SideEffect.BecomeLeader)
                }
                on<Event.HigherTermServer> {
                    transitionTo(State.Follower, SideEffect.BecomeFollower)
                }
            }
            onTransition {
                val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
                val sideEffect = validTransition.sideEffect ?: return@onTransition
                changeState(sideEffect)
            }
        }
    }
}
