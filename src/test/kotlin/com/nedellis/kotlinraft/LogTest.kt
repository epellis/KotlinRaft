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
})
