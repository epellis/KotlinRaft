package com.nedellis.kotlinraft

import java.lang.Exception
import kotlin.math.min

// TODO: Investigate making everything immutable
data class Log(
    private var log: MutableList<Entry> = mutableListOf(),
    private var term: Int = 0,
    private var commitIndex: Int = 0
) {
    // Attempt to append entries to the log
    fun append(req: AppendRequest): AppendResponse {
        // 1. If term < currentTerm, reply false
        if (req.term < term) {
            return AppendResponse.newBuilder().setSuccess(false).setTerm(term).build()
        }

        // 2. If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reply false
        if (log.size < req.prevLogIndex || (req.prevLogIndex > 0 && log[req.prevLogIndex].term != req.prevLogTerm)) {
            return AppendResponse.newBuilder().setSuccess(false).setTerm(term).build()
        }

        // 3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        // TODO: Skip for now, implement later for faster append

        // 4. Append any new entries not already in log
        log = log.subList(0, req.prevLogIndex)
        log.addAll(req.entriesList)

        // 5. If leader's commit is > commitIndex, set commitIndex = min(leaderCommit, index of new last entry)
        if (req.leaderCommit > commitIndex) {
            commitIndex = min(req.leaderCommit, log.size)
        }

        return AppendResponse.newBuilder().setSuccess(true).setTerm(term).build()
    }

    // Attempt to retrieve entries from the log
    fun get(key: Key): GetStatus {
        val res = find(key).getOrNull()
        return if (res != null) {
            GetStatus.newBuilder().setStatus(GetStatus.Status.OK).setValue(res).build()
        } else {
            GetStatus.newBuilder().setStatus(GetStatus.Status.NOT_FOUND).build()
        }
    }

    // Starting at the back of the log, try to find the first entry, and return success if not deleted
    // TODO: Do not examine entries that are not committed
    // TODO: Speedup with checkpoint hash table
    private fun find(key: Key): Result<String> {
        for (entry in log.reversed()) {
            if (entry.key == key.key && entry.action == Entry.Action.APPEND) return Result.success(entry.key)
            if (entry.key == key.key && entry.action == Entry.Action.DELETE) return Result.failure(Exception())
        }
        return Result.failure(Exception())
    }

    fun slice(range: IntRange): List<Entry> = log.slice(range)
}