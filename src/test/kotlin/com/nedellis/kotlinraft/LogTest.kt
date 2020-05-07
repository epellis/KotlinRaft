package com.nedellis.kotlinraft

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import java.lang.IllegalArgumentException

class LogTest : StringSpec({
    "empty log refresh" {
        val req = mockk<AppendRequest>()

        every { req.term } returns 0
        every { req.leaderCommit } returns 0
        every { req.prevLogIndex } returns 0
        every { req.entriesList } returns listOf()

        val log = Log()

        log.append(req) shouldBe AppendResponse.newBuilder()
            .setTerm(0)
            .setSuccess(true)
            .build()
    }

    "single log refresh" {
        val req = mockk<AppendRequest>()
        val entry = mockk<Entry>()

        every { req.term } returns 0
        every { req.leaderCommit } returns 0
        every { req.prevLogIndex } returns 0
        every { req.entriesList } returns listOf(entry)

        val log = Log()

        log.append(req) shouldBe AppendResponse.newBuilder()
            .setTerm(0)
            .setSuccess(true)
            .build()
    }

    "many log refresh" {
        val req = mockk<AppendRequest>()
        val entry = mockk<Entry>()

        every { req.term } returns 0
        every { req.leaderCommit } returns 0
        every { req.prevLogIndex } returns 0
        every { req.entriesList } returns listOf(entry, entry, entry)

        val log = Log()

        log.append(req) shouldBe AppendResponse.newBuilder()
            .setTerm(0)
            .setSuccess(true)
            .build()
    }

    "test fresh build vote" {
        val candidateID = 0
        val currentTerm = 0
        val internalLog = mockk<MutableList<Entry>>()

        every { internalLog.size } returns 0
        every { internalLog.isEmpty() } returns true

        val log = Log(term = currentTerm, log = internalLog)

        val req = log.buildVoteRequest(candidateID)
        req.term shouldBe currentTerm
        req.candidateID shouldBe candidateID
        req.lastLogIndex shouldBe internalLog.size
        req.lastLogTerm shouldBe 0
    }

    "test ongoing build vote" {
        val candidateID = 0
        val currentTerm = 5
        val lastEntryTerm = 3
        val internalLog = mockk<MutableList<Entry>>()
        val lastEntry = mockk<Entry>()

        every { internalLog.size } returns 2
        every { internalLog.isEmpty() } returns false
        every { internalLog.last() } returns lastEntry
        every { internalLog[1] } returns lastEntry
        every { lastEntry.term } returns lastEntryTerm

        val log = Log(term = currentTerm, log = internalLog)

        val req = log.buildVoteRequest(candidateID)
        req.term shouldBe currentTerm
        req.candidateID shouldBe candidateID
        req.lastLogIndex shouldBe internalLog.size
        req.lastLogTerm shouldBe lastEntryTerm
    }

    "test fresh append request" {
        val leaderID = 0
        val currentTerm = 0
        val prevLogIndex = 0
        val leaderCommit = 0
        val internalLog = emptyList<Entry>().toMutableList()

        val log = Log(term = currentTerm, log = internalLog)

        val req = log.buildAppendRequest(leaderID, prevLogIndex, leaderCommit)
        req.term shouldBe currentTerm
        req.leaderID shouldBe leaderID
        req.prevLogIndex shouldBe prevLogIndex
        req.entriesList.isEmpty() shouldBe true
        req.leaderCommit shouldBe leaderCommit
    }

    // Add the additional entry at index 1 to the follower's list
    "test ongoing append request" {
        val leaderID = 0
        val currentTerm = 5
        val prevLogIndex = 1
        val leaderCommit = 2
        val lastEntryTerm = 3
        val lastEntry = mockk<Entry>()
        val internalLog = listOf(lastEntry, lastEntry).toMutableList()

        every { lastEntry.term } returns lastEntryTerm

        val log = Log(term = currentTerm, log = internalLog)

        val req = log.buildAppendRequest(leaderID, prevLogIndex, leaderCommit)
        req.term shouldBe currentTerm
        req.leaderID shouldBe leaderID
        req.prevLogIndex shouldBe prevLogIndex
        req.entriesList shouldBe listOf(lastEntry)
        req.leaderCommit shouldBe leaderCommit
    }

    "test fresh vote request" {
        val currentTerm = 0
        val candidateID = 0
        val req = mockk<VoteRequest>()

        every { req.candidateID } returns candidateID
        every { req.term } returns currentTerm
        every { req.lastLogIndex } returns 0
        every { req.lastLogTerm } returns 0

        val log = Log(term = currentTerm, votedFor = null)
        val res = log.vote(req)
        res.term shouldBe currentTerm
        res.voteGranted shouldBe true
    }

    "test ongoing vote request" {
        val currentTerm = 0
        val candidateID = 32
        val req = mockk<VoteRequest>()

        every { req.candidateID } returns candidateID
        every { req.term } returns currentTerm
        every { req.lastLogIndex } returns 0
        every { req.lastLogTerm } returns 0

        val log = Log(term = currentTerm, votedFor = candidateID)
        val res = log.vote(req)
        res.term shouldBe currentTerm
        res.voteGranted shouldBe true
    }

    "test ongoing vote request bad term" {
        val currentTerm = 5
        val candidateID = 32
        val req = mockk<VoteRequest>()

        every { req.candidateID } returns candidateID
        every { req.term } returns 0
        every { req.lastLogIndex } returns 0
        every { req.lastLogTerm } returns 1

        val log = Log(term = currentTerm, votedFor = candidateID)

        val res = log.vote(req)
        res.term shouldBe currentTerm
        res.voteGranted shouldBe false
    }

    "test ongoing vote request bad log size" {
        val currentTerm = 5
        val candidateID = 32
        val internalLogEntry = mockk<Entry>()
        val internalLog = listOf(internalLogEntry).toMutableList()
        val req = mockk<VoteRequest>()

        every { internalLogEntry.term } returns 1
        every { req.candidateID } returns candidateID
        every { req.term } returns 0
        every { req.lastLogIndex } returns 0
        every { req.lastLogTerm } returns 1

        val log = Log(term = currentTerm, votedFor = candidateID, log = internalLog)

        val res = log.vote(req)
        res.term shouldBe currentTerm
        res.voteGranted shouldBe false
    }

    // TODO: Test multiple vote requests
    // TODO: Request builders or at least constant definitions

    "increment term resets votedFor" {
        // TODO: Decouple so not having to test voting to see that can vote
        val currentVotedFor = 32
        val currentTerm = 16
        val newTerm = 18
        val voteRequest = mockk<VoteRequest>()
        val log = Log(votedFor = currentVotedFor, term = currentTerm)

        every { voteRequest.term } returns newTerm
        every { voteRequest.candidateID } returns 0
        every { voteRequest.lastLogIndex } returns 0
        every { voteRequest.lastLogTerm } returns 0

        log.changeTerm(newTerm)
        val res = log.vote(voteRequest)

        res.voteGranted shouldBe true
        res.term shouldBe newTerm
    }

    "can only increment term" {
        val currentTerm = 16
        val lowerTerm = 14
        val sameTerm = 16
        val higherTerm = 18
        val log = Log(term = currentTerm)

        shouldThrow<IllegalArgumentException> {
            log.changeTerm(lowerTerm)
        }
        shouldThrow<IllegalArgumentException> {
            log.changeTerm(sameTerm)
        }
        shouldNotThrow<IllegalArgumentException> {
            log.changeTerm(higherTerm)
        }

        log.term() shouldBe higherTerm
    }
})
