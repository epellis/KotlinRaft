package com.nedellis.kotlinraft

import com.github.mustachejava.DefaultMustacheFactory
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.http.ContentType
import io.ktor.mustache.Mustache
import io.ktor.mustache.MustacheContent
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

data class User(val name: String, val email: String)

fun runDashboard(port: Int) {
    val server = embeddedServer(Netty, port) {
        install(Mustache) {
            mustacheFactory = DefaultMustacheFactory("templates")
        }

        routing {
            get("/") {
                val user = User("username", "user@example.com")
                call.respond(MustacheContent("hello.hbs", mapOf("user" to user)))
            }
        }
    }

    server.start()
}
