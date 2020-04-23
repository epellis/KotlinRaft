package com.nedellis.kotlinraft

import com.github.mustachejava.DefaultMustacheFactory
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.mustache.Mustache
import io.ktor.mustache.MustacheContent
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

fun runDashboard(port: Int) {
    val server = embeddedServer(Netty, port) {
        install(Mustache) {
            mustacheFactory = DefaultMustacheFactory("templates")
        }

        routing {
            get("/") {
//                val user = User("username", "user@example.com")
                call.respond(MustacheContent("hello.hbs", null))
            }
        }
    }

    server.start()
}
