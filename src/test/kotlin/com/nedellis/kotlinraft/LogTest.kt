package com.nedellis.kotlinraft

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.properties.nextPrintableString
import kotlin.random.Random

fun buildMockVoteRequest(term: Int, candidateID: Int, lastLogIndex: Int, lastLogTerm: Int): VoteRequest {
    return VoteRequest.newBuilder()
        .setTerm(term)
        .setCandidateID(candidateID)
        .setLastLogIndex(lastLogIndex)
        .setLastLogTerm(lastLogTerm)
        .build()
}

fun buildMockEntry(
    term: Int,
    key: String = Random.nextPrintableString(10),
    value: String = Random.nextPrintableString(10),
    action: Entry.Action = Entry.Action.APPEND
): Entry {
    return Entry.newBuilder()
        .setKey(key)
        .setValue(value)
        .setAction(action)
        .setTerm(term)
        .build()
}

class LogTest : StringSpec({
    "Test deny vote if request term not up to date" {
        val term = 0
        val candidateID = Random.nextInt()
        val lastLogIndex = Random.nextInt()
        val lastLogTerm = Random.nextInt()
        val id = Random.nextInt()
        val voteRequest = buildMockVoteRequest(term, candidateID, lastLogIndex, lastLogTerm)

        val logTerm = 10
        val log = Log(id, term = logTerm)

        val (newLog, voteResponse) = log.vote(voteRequest)

        voteResponse.voteGranted shouldBe false
        newLog.term shouldBe logTerm
        newLog.votedFor shouldBe null
    }

    "Test deny vote if already voted for another candidate" {
        val term = Random.nextInt()
        val candidateID = Random.nextInt()
        val lastLogIndex = 0
        val lastLogTerm = 0
        val id = Random.nextInt()
        val voteRequest = buildMockVoteRequest(term, candidateID, lastLogIndex, lastLogTerm)

        val votedFor = Random.nextInt()
        val log = Log(id, term = term, votedFor = votedFor)

        val (newLog, voteResponse) = log.vote(voteRequest)

        voteResponse.voteGranted shouldBe false
        newLog.term shouldBe term
        newLog.votedFor shouldBe votedFor
    }

    "Test deny vote if log is not up to date" {
        val term = Random.nextInt()
        val logInternalLog = listOf(buildMockEntry(term), buildMockEntry(term))

        val candidateID = Random.nextInt()
        val lastLogIndex = logInternalLog.size - 1
        val lastLogTerm = term - 1
        val id = Random.nextInt()
        val voteRequest = buildMockVoteRequest(term, candidateID, lastLogIndex, lastLogTerm)
        val log = Log(id, term = term, log = logInternalLog)

        val (newLog, voteResponse) = log.vote(voteRequest)

        voteResponse.voteGranted shouldBe false
        newLog.term shouldBe term
        newLog.log shouldBe logInternalLog
        newLog.votedFor shouldBe null
    }

    "Test confirm vote if request is valid" {
        val term = Random.nextInt()
        val logInternalLog = listOf(buildMockEntry(term), buildMockEntry(term))

        val candidateID = Random.nextInt()
        val lastLogIndex = logInternalLog.size
        val id = Random.nextInt()
        val voteRequest = buildMockVoteRequest(term, candidateID, lastLogIndex, term)
        val log = Log(id, term = term, log = logInternalLog)

        val (newLog, voteResponse) = log.vote(voteRequest)

        voteResponse.voteGranted shouldBe true
        newLog.term shouldBe term
        newLog.log shouldBe logInternalLog
        newLog.votedFor shouldBe candidateID
    }

    "Test initializes to voting for no one" {
        Log(Random.nextInt()).votedFor shouldBe null
    }

    "Test vote for self" {
        val id = Random.nextInt()

        val log = Log(id)

        log.votedFor shouldBe null
        log.voteForSelf().votedFor shouldBe id
    }

    "Test change term" {
        val id = Random.nextInt()
        val term = Random.nextInt()
        val nextTerm = Random.nextInt() + 10

        val log = Log(id, term = term)

        log.term shouldBe term
        log.changeTerm(nextTerm).term shouldBe nextTerm
    }
})
