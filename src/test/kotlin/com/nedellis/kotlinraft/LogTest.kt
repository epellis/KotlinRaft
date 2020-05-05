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
})
