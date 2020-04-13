package com.nedellis.kotlinraft

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int

class RunRaft : CliktCommand() {
    val port: Int by option(help = "Starting port to run off of").int().default(8000)
    val clients: Int by option(help = "Number of raft clients to run").int().default(1)

    override fun run() {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");

        Discovery(port).run()
        for (clientPort in port + 1..port + clients + 1) {
            Raft(clientPort, port).run()
        }
    }
}

fun main(args: Array<String>) = RunRaft().main(args)
