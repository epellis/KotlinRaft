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
        ctx: Contexts,
        toFollower: () -> Unit,
        toCandidate: () -> Unit,
        toLeader: () -> Unit
    ): StateMachine<State, Event, SideEffect> {
        return StateMachine.create<State, Event, SideEffect> {
            initialState(State.Follower)
            state<State.Follower> {
                on<Event.FollowerTimeout> {
                    transitionTo(State.Candidate, SideEffect.BecomeCandidate)
                }
            }
            state<State.Follower> {
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
            state<State.Follower> {
                on<Event.LeaderRefreshTimer> {
                    transitionTo(State.Leader, SideEffect.BecomeLeader)
                }
                on<Event.HigherTermServer> {
                    transitionTo(State.Follower, SideEffect.BecomeFollower)
                }
            }
            onTransition {
                val validTransition = it as? StateMachine.Transition.Valid ?: return@onTransition
                ctx.refresh()
                when (validTransition.sideEffect) {
                    SideEffect.BecomeFollower -> toFollower()
                    SideEffect.BecomeCandidate -> toCandidate()
                    SideEffect.BecomeLeader -> toLeader()
                }
            }
        }
    }
}
