package com.nedellis.kotlinraft

import java.lang.Exception
import java.lang.IllegalArgumentException
import kotlin.math.min
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger

// TODO: Investigate making everything immutable
data class Log(
    private var log: MutableList<Entry> = mutableListOf(),
    private var term: Int = 0,
    private var votedFor: Int? = null,
    private var commitIndex: Int = 0,
    private var leaderCommit: Int = 0,
    private val mutex: Mutex = Mutex(),
    private val logger: Logger? = null
) {
    // Attempt to append entries to the log
    suspend fun append(req: AppendRequest): AppendResponse = mutex.withLock {
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

    suspend fun term(): Int = mutex.withLock {
        return term
    }

    suspend fun leaderCommit(): Int = mutex.withLock {
        return leaderCommit
    }

    // Attempt to retrieve entries from the log
    suspend fun get(key: Key): GetStatus = mutex.withLock {
        val res = find(key).getOrNull()
        return if (res != null) {
            GetStatus.newBuilder().setStatus(GetStatus.Status.OK).setValue(res).build()
        } else {
            GetStatus.newBuilder().setStatus(GetStatus.Status.NOT_FOUND).build()
        }
    }

    suspend fun changeTerm(newTerm: Int) = mutex.withLock {
        if (newTerm <= term) {
            throw IllegalArgumentException("$newTerm is not greater than $term")
        }
        votedFor = null
        term = newTerm
    }

    suspend fun voteForSelf(myId: Int) = mutex.withLock {
        // TODO: Assert that have not voted for anyone else
        votedFor = myId
    }

    suspend fun vote(req: VoteRequest): VoteResponse = mutex.withLock {
        // 1. Reply false if term < currentTerm
        if (req.term < term) {
            logger?.info("Denying vote for ${req.candidateID} because term is too low")
            return VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build()
        }

        // 2.1 If already voted for another candidate, reply false
        if (votedFor != null && votedFor != req.candidateID) {
            logger?.info("Denying vote for ${req.candidateID} because already voted for $votedFor")
            return VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build()
        }

        // 2.2 Reply true if candidate's log is at least as up-to-date as receiver's log
        val lastLogTerm = if (log.isEmpty()) 0 else log.last().term
        return if (req.term >= term && req.lastLogIndex >= log.size && req.lastLogTerm >= lastLogTerm) {
            votedFor = req.candidateID
            VoteResponse.newBuilder().setTerm(term).setVoteGranted(true).build()
        } else {
            logger?.info("Denying vote for ${req.candidateID} because it's index is ${req.lastLogIndex}, mine is ${log.size}")
            logger?.info("Denying vote for ${req.candidateID} because it's term is ${req.lastLogTerm}, mine is $lastLogTerm")
            VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build()
        }
    }

    fun buildAppendRequest(myId: Int, prevLogIndex: Int, leaderCommit: Int): AppendRequest {
        val prevLogTerm = if (log.isEmpty()) 0 else log.last().term

        val delta = if (log.isEmpty()) listOf() else log.slice(prevLogIndex until log.size)

        return AppendRequest.newBuilder()
            .setTerm(term)
            .setLeaderID(myId)
            .setPrevLogIndex(prevLogIndex)
            .setPrevLogTerm(prevLogTerm)
            .addAllEntries(delta)
            .setLeaderCommit(leaderCommit)
            .build()
    }

    fun buildVoteRequest(myId: Int): VoteRequest {
        val lastLogTerm = if (log.isEmpty()) 0 else log.last().term
        return VoteRequest.newBuilder()
            .setCandidateID(myId)
            .setTerm(term)
            .setLastLogIndex(log.size)
            .setLastLogTerm(lastLogTerm)
            .build()
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
}
