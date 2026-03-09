package com.patryktargosinski.webcrawler.testsupport

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

class FixtureServer(private val handler: (HttpExchange) -> Unit) : AutoCloseable {
    private val executor = Executors.newCachedThreadPool()
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)

    init {
        server.executor = executor
        server.createContext("/") { exchange -> handler(exchange) }
        server.start()
    }

    val baseUrl: String
        get() = "http://127.0.0.1:${server.address.port}"

    override fun close() {
        server.stop(0)
        executor.shutdownNow()
    }
}

fun HttpExchange.respond(
    statusCode: Int,
    body: String,
    contentType: String = "text/html; charset=utf-8",
) {
    val bytes = body.toByteArray(StandardCharsets.UTF_8)
    responseHeaders.set("Content-Type", contentType)
    sendResponseHeaders(statusCode, bytes.size.toLong())
    responseBody.use { stream ->
        stream.write(bytes)
    }
}
