package com.nedellis.kotlinraft

import java.lang.Exception
import kotlin.math.min

// Immutable log data structure. All mutable operations will return a new copy of the log
// TODO: Investigate using a timestamp to make sure concurrent log accesses don't go back in time
data class Log(
    val id: Int,
    val log: List<Entry> = listOf(),
    val term: Int = 0,
    val commitIndex: Int = 0,
    val votedFor: Int? = null // Represents who you voted for in election, if convert to follower set to leader
) {
    // TODO: Checkpoint the log when returning from append
    @ExperimentalStdlibApi
    fun append(req: AppendRequest): Pair<Log, AppendResponse> {
        // 1. If term < currentTerm, reply false
        if (req.term < term) {
            return Pair(this, AppendResponse.newBuilder().setSuccess(false).setTerm(term).build())
        }

        // 2. If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm, reply false
        if (log.size < req.prevLogIndex || (req.prevLogIndex > 0 && log[req.prevLogIndex].term != req.prevLogTerm)) {
            return Pair(this, AppendResponse.newBuilder().setSuccess(false).setTerm(term).build())
        }

        // 3. If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        // TODO: Skip for now, implement later for faster append

        // 4. Append any new entries not already in log
        val newLog = buildList {
            log.slice(0..req.prevLogIndex)
            addAll(req.entriesList)
        }

        // 5. If leader's commit is > commitIndex, set commitIndex = min(leaderCommit, index of new last entry)
        val newCommitIndex = if (req.leaderCommit > commitIndex) {
            min(req.leaderCommit, log.size)
        } else commitIndex

        return Pair(
            this.copy(log = newLog, commitIndex = newCommitIndex),
            AppendResponse.newBuilder().setSuccess(true).setTerm(term).build()
        )
    }

    @ExperimentalStdlibApi
    fun update(req: Entry): Pair<Log, UpdateStatus> {
        val newLog = buildList {
            log
            add(req)
        }

        return Pair(
            this.copy(log = newLog),
            UpdateStatus.newBuilder().setStatus(UpdateStatus.Status.OK).build()
        )
    }

    fun vote(req: VoteRequest): Pair<Log, VoteResponse> {
        // 1. Reply false if term < currentTerm
        if (req.term < term) {
            return Pair(this, VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build())
        }

        // 2.1 If already voted for another candidate, reply false
        if (votedFor != null && votedFor != req.candidateID) {
            return Pair(this, VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build())
        }

        // 2.2 Reply true if candidate's log is at least as up-to-date as receiver's log
        val lastLogTerm = if (log.isEmpty()) 0 else log.last().term
        return if (req.term >= term && req.lastLogIndex >= log.size && req.lastLogTerm >= lastLogTerm) {
            val newVotedFor = req.candidateID
            Pair(
                this.copy(votedFor = newVotedFor),
                VoteResponse.newBuilder().setTerm(term).setVoteGranted(true).build()
            )
        } else {
            Pair(this, VoteResponse.newBuilder().setTerm(term).setVoteGranted(false).build())
        }
    }

    fun voteForSelf(): Log = this.copy(votedFor = id)

    fun changeTerm(newTerm: Int): Log = this.copy(term = newTerm)

    fun buildAppendRequest(prevLogIndex: Int, leaderCommit: Int): AppendRequest {
        val prevLogTerm = if (log.isEmpty()) 0 else log.last().term

        val delta = if (log.isEmpty()) listOf() else log.slice(prevLogIndex until log.size)

        return AppendRequest.newBuilder()
            .setTerm(term)
            .setLeaderID(id)
            .setPrevLogIndex(prevLogIndex)
            .setPrevLogTerm(prevLogTerm)
            .addAllEntries(delta)
            .setLeaderCommit(leaderCommit)
            .build()
    }

    fun buildVoteRequest(): VoteRequest {
        val lastLogTerm = if (log.isEmpty()) 0 else log.last().term
        return VoteRequest.newBuilder()
            .setCandidateID(id)
            .setTerm(term)
            .setLastLogIndex(log.size)
            .setLastLogTerm(lastLogTerm)
            .build()
    }

    fun get(key: Key): GetStatus {
        val res = find(key.key).getOrNull()
        return if (res != null) {
            GetStatus.newBuilder().setStatus(GetStatus.Status.OK).setValue(res).build()
        } else {
            GetStatus.newBuilder().setStatus(GetStatus.Status.NOT_FOUND).build()
        }
    }

    // Starting at the back of the log, try to find the first entry, and return success if not deleted
    // TODO: Do not examine entries that are not committed
    // TODO: Speedup with checkpoint hash table
    private fun find(key: String): Result<String> {
        for (entry in log.reversed()) {
            if (entry.key == key && entry.action == Entry.Action.APPEND) return Result.success(entry.value)
            if (entry.key == key && entry.action == Entry.Action.DELETE) return Result.failure(Exception())
        }
        return Result.failure(Exception())
    }
}
