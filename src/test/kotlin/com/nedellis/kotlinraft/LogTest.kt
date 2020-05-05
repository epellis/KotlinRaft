package com.nedellis.kotlinraft

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk

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
})
