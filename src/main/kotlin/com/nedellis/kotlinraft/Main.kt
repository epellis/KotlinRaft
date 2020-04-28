package com.nedellis.kotlinraft

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class RunRaft : CliktCommand() {
    private val port: Int by option(help = "Starting port to run off of").int().default(7000)
    private val clients: Int by option(help = "Number of raft clients to run").int().default(1)

    override fun run() {
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO")
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
        System.setProperty(org.slf4j.impl.SimpleLogger.SHOW_DATE_TIME_KEY, "TRUE")

        val clients = (port until port + clients).toList()
        val rafts = clients.map {
            Raft(it, clients)
        }

        runBlocking {
            for (raft in rafts) {
                launch {
                    raft.run()
                }
            }
        }
    }
}

fun main(args: Array<String>) = RunRaft().main(args)
