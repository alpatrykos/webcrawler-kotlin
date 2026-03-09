package com.patryktargosinski.webcrawler

import com.patryktargosinski.webcrawler.cli.CrawlerCli
import com.patryktargosinski.webcrawler.db.SqliteStore
import com.patryktargosinski.webcrawler.model.HttpCookie
import com.patryktargosinski.webcrawler.model.HttpHeader
import com.patryktargosinski.webcrawler.model.JobStatus
import com.patryktargosinski.webcrawler.service.CrawlerService
import com.patryktargosinski.webcrawler.testsupport.FixtureServer
import com.patryktargosinski.webcrawler.testsupport.respond
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CrawlerIntegrationTest {
    @Test
    fun `enqueue and status commands show queued job`() {
        val tempDir = createTempDirectory("crawler-cli-status")
        val dbPath = tempDir.resolve("crawler.db")

        val enqueueOutput =
            captureStdout {
                CrawlerCli().parse(
                    listOf(
                        "enqueue",
                        "--db",
                        dbPath.toString(),
                        "--seed-url",
                        "https://example.com",
                    ),
                )
            }
        val statusOutput =
            captureStdout {
                CrawlerCli().parse(
                    listOf(
                        "status",
                        "--db",
                        dbPath.toString(),
                    ),
                )
            }

        assertContains(enqueueOutput, "Enqueued crawl job 1")
        assertContains(statusOutput, "Job 1")
        assertContains(statusOutput, "QUEUED")
        assertContains(statusOutput, "https://example.com/")
    }

    @Test
    fun `browser crawler follows javascript rendered links`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nAllow: /\n", "text/plain")
                "/" ->
                    exchange.respond(
                        200,
                        """
                        <html>
                          <head>
                            <title>Home</title>
                            <script>
                              setTimeout(() => {
                                const link = document.createElement('a');
                                link.href = '/rendered';
                                link.textContent = 'Rendered';
                                document.body.appendChild(link);
                              }, 50);
                            </script>
                          </head>
                          <body>
                            <a href="/static">Static</a>
                          </body>
                        </html>
                        """.trimIndent(),
                    )
                "/static" -> exchange.respond(200, "<html><head><title>Static</title></head><body>Static</body></html>")
                "/rendered" -> exchange.respond(200, "<html><head><title>Rendered</title></head><body>Rendered</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val dbPath = createTempDirectory("crawler-js").resolve("crawler.db")
            val service = CrawlerService(dbPath)
            val jobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    maxDepth = 1,
                    maxPages = 10,
                    delayMs = 0,
                    renderWaitMs = 250,
                    allowSubdomains = false,
                    headers = emptyList(),
                    cookies = emptyList(),
                )

            val job = service.runNext()
            val pages = service.store().listPages(jobId)

            assertEquals(JobStatus.COMPLETED, job?.status)
            assertEquals(
                setOf("${server.baseUrl}/", "${server.baseUrl}/rendered", "${server.baseUrl}/static"),
                pages.map { it.normalizedUrl }.toSet(),
            )
            assertEquals("Home", pages.first { it.normalizedUrl == "${server.baseUrl}/" }.title)
        }
    }

    @Test
    fun `robots disallow on seed fails the job`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nDisallow: /private\n", "text/plain")
                "/private" -> exchange.respond(200, "<html><head><title>Private</title></head><body>Private</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val service = CrawlerService(createTempDirectory("crawler-robots-seed").resolve("crawler.db"))
            val jobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/private",
                    maxDepth = 1,
                    maxPages = 10,
                    delayMs = 0,
                    renderWaitMs = 0,
                    allowSubdomains = false,
                    headers = emptyList(),
                    cookies = emptyList(),
                )

            val job = service.runNext()
            val pages = service.store().listPages(jobId)

            assertEquals(JobStatus.FAILED, job?.status)
            assertContains(job?.errorMessage.orEmpty(), "robots.txt")
            assertTrue(pages.isEmpty())
        }
    }

    @Test
    fun `robots disallowed child links are discovered but not crawled`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nDisallow: /private\n", "text/plain")
                "/" ->
                    exchange.respond(
                        200,
                        """
                        <html><head><title>Index</title></head><body>
                          <a href="/public">Public</a>
                          <a href="/private">Private</a>
                        </body></html>
                        """.trimIndent(),
                    )
                "/public" -> exchange.respond(200, "<html><head><title>Public</title></head><body>Public</body></html>")
                "/private" -> exchange.respond(200, "<html><head><title>Private</title></head><body>Private</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val service = CrawlerService(createTempDirectory("crawler-robots-child").resolve("crawler.db"))
            val jobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    maxDepth = 1,
                    maxPages = 10,
                    delayMs = 0,
                    renderWaitMs = 0,
                    allowSubdomains = false,
                    headers = emptyList(),
                    cookies = emptyList(),
                )

            service.runNext()
            val pages = service.store().listPages(jobId)
            val links = service.store().listLinks(jobId)

            assertEquals(setOf("${server.baseUrl}/", "${server.baseUrl}/public"), pages.map { it.normalizedUrl }.toSet())
            assertTrue(links.any { it.normalizedTargetUrl == "${server.baseUrl}/private" })
        }
    }

    @Test
    fun `headers and cookies are sent with browser requests`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nAllow: /\n", "text/plain")
                "/" -> {
                    val token = exchange.requestHeaders.getFirst("X-Token")
                    val cookieHeader = exchange.requestHeaders.getFirst("Cookie").orEmpty()
                    if (token == "secret" && "session=abc" in cookieHeader) {
                        exchange.respond(
                            200,
                            "<html><head><title>Auth</title></head><body><a href=\"/ok\">OK</a></body></html>",
                        )
                    } else {
                        exchange.respond(401, "<html><head><title>Denied</title></head><body>Denied</body></html>")
                    }
                }
                "/ok" -> exchange.respond(200, "<html><head><title>OK</title></head><body>OK</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val service = CrawlerService(createTempDirectory("crawler-auth").resolve("crawler.db"))
            val jobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    maxDepth = 1,
                    maxPages = 10,
                    delayMs = 0,
                    renderWaitMs = 0,
                    allowSubdomains = false,
                    headers = listOf(HttpHeader("X-Token", "secret")),
                    cookies = listOf(HttpCookie("session", "abc")),
                )

            val job = service.runNext()
            val pages = service.store().listPages(jobId)

            assertEquals(JobStatus.COMPLETED, job?.status)
            assertTrue(pages.all { it.httpStatus == 200 })
            assertEquals(setOf("${server.baseUrl}/", "${server.baseUrl}/ok"), pages.map { it.normalizedUrl }.toSet())
        }
    }

    @Test
    fun `duplicate links only crawl one normalized page`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nAllow: /\n", "text/plain")
                "/" ->
                    exchange.respond(
                        200,
                        """
                        <html><head><title>Index</title></head><body>
                          <a href="/a">Absolute path</a>
                          <a href="./a">Relative path</a>
                          <a href="http://127.0.0.1:${exchange.localAddress.port}/a#fragment">Fragment variant</a>
                        </body></html>
                        """.trimIndent(),
                    )
                "/a" -> exchange.respond(200, "<html><head><title>A</title></head><body>A</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val service = CrawlerService(createTempDirectory("crawler-dedupe").resolve("crawler.db"))
            val jobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    maxDepth = 1,
                    maxPages = 10,
                    delayMs = 0,
                    renderWaitMs = 0,
                    allowSubdomains = false,
                    headers = emptyList(),
                    cookies = emptyList(),
                )

            service.runNext()
            val pages = service.store().listPages(jobId)
            val links = service.store().listLinks(jobId)

            assertEquals(setOf("${server.baseUrl}/", "${server.baseUrl}/a"), pages.map { it.normalizedUrl }.toSet())
            assertEquals(3, links.size)
        }
    }

    @Test
    fun `runNext marks stale running jobs failed before processing next queued job`() {
        FixtureServer { exchange ->
            when (exchange.requestURI.path) {
                "/robots.txt" -> exchange.respond(200, "User-agent: *\nAllow: /\n", "text/plain")
                "/" -> exchange.respond(200, "<html><head><title>Ready</title></head><body>Ready</body></html>")
                else -> exchange.respond(404, "missing", "text/plain")
            }
        }.use { server ->
            val dbPath = createTempDirectory("crawler-stale").resolve("crawler.db")
            val store = SqliteStore(dbPath)
            val firstJobId =
                store.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    seedHost = "127.0.0.1",
                    allowSubdomains = false,
                    maxDepth = 0,
                    maxPages = 1,
                    delayMs = 0,
                    renderWaitMs = 0,
                    headers = emptyList(),
                    cookies = emptyList(),
                )
            val claimed = store.claimNextQueuedJob()
            assertNotNull(claimed)
            assertEquals(firstJobId, claimed.id)

            val service = CrawlerService(dbPath)
            val secondJobId =
                service.enqueueJob(
                    seedUrl = "${server.baseUrl}/",
                    maxDepth = 0,
                    maxPages = 1,
                    delayMs = 0,
                    renderWaitMs = 0,
                    allowSubdomains = false,
                    headers = emptyList(),
                    cookies = emptyList(),
                )

            val processedJob = service.runNext()
            val summaries = service.listStatus().associateBy { it.id }

            assertEquals(secondJobId, processedJob?.id)
            assertEquals(JobStatus.FAILED, summaries[firstJobId]?.status)
            assertEquals(JobStatus.COMPLETED, summaries[secondJobId]?.status)
        }
    }

    private fun captureStdout(block: () -> Unit): String {
        val originalOut = System.out
        val buffer = ByteArrayOutputStream()
        val printStream = PrintStream(buffer, true, StandardCharsets.UTF_8)
        return try {
            System.setOut(printStream)
            block()
            buffer.toString(StandardCharsets.UTF_8)
        } finally {
            System.setOut(originalOut)
        }
    }
}
