package com.patryktargosinski.webcrawler.cli

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.NoOpCliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.github.ajalt.clikt.parameters.types.long
import com.patryktargosinski.webcrawler.model.HttpCookie
import com.patryktargosinski.webcrawler.model.HttpHeader
import com.patryktargosinski.webcrawler.service.CrawlerService
import com.patryktargosinski.webcrawler.service.formatStatus
import java.nio.file.Path

class CrawlerCli : NoOpCliktCommand(name = "webcrawler", printHelpOnEmptyArgs = true) {
    init {
        subcommands(
            EnqueueCommand(),
            RunNextCommand(),
            RunAllCommand(),
            StatusCommand(),
        )
    }
}

private class EnqueueCommand : CliktCommand(name = "enqueue", help = "Queue a crawl job.") {
    private val seedUrl by option("--seed-url", help = "Seed URL to crawl.").required()
    private val dbPath by option("--db", help = "Path to the SQLite database.").default("./crawler.db")
    private val maxDepth by option("--max-depth", help = "Maximum crawl depth.").int().default(3)
    private val maxPages by option("--max-pages", help = "Maximum number of pages to fetch.").int().default(100)
    private val delayMs by option("--delay-ms", help = "Delay between page fetches in milliseconds.").long().default(1000)
    private val renderWaitMs by option("--render-wait-ms", help = "Extra wait after DOMContentLoaded in milliseconds.")
        .long()
        .default(1000)
    private val allowSubdomains by option("--allow-subdomains", help = "Allow crawling subdomains of the seed host.")
        .flag(default = false)
    private val headers by option("--header", help = "Additional header in 'Name: value' format.").multiple()
    private val cookies by option("--cookie", help = "Cookie in 'name=value' format.").multiple()

    override fun run() {
        val service = CrawlerService(Path.of(dbPath))
        val jobId =
            service.enqueueJob(
                seedUrl = seedUrl,
                maxDepth = maxDepth,
                maxPages = maxPages,
                delayMs = delayMs,
                renderWaitMs = renderWaitMs,
                allowSubdomains = allowSubdomains,
                headers = headers.map(::parseHeader),
                cookies = cookies.map(::parseCookie),
            )

        echo("Enqueued crawl job $jobId")
    }
}

private class RunNextCommand : CliktCommand(name = "run-next", help = "Run the next queued crawl job.") {
    private val dbPath by option("--db", help = "Path to the SQLite database.").default("./crawler.db")

    override fun run() {
        val service = CrawlerService(Path.of(dbPath))
        val job = service.runNext()
        if (job == null) {
            echo("No queued crawl jobs found.")
        } else {
            echo("Processed crawl job ${job.id} with status ${job.status.name}")
        }
    }
}

private class RunAllCommand : CliktCommand(name = "run-all", help = "Run all queued crawl jobs.") {
    private val dbPath by option("--db", help = "Path to the SQLite database.").default("./crawler.db")

    override fun run() {
        val service = CrawlerService(Path.of(dbPath))
        val jobs = service.runAll()
        echo("Processed ${jobs.size} crawl job(s).")
    }
}

private class StatusCommand : CliktCommand(name = "status", help = "Show crawl job status.") {
    private val dbPath by option("--db", help = "Path to the SQLite database.").default("./crawler.db")
    private val jobId by option("--job-id", help = "Specific crawl job ID to inspect.").long()

    override fun run() {
        val service = CrawlerService(Path.of(dbPath))
        echo(formatStatus(service.listStatus(jobId)))
    }
}

private fun parseHeader(raw: String): HttpHeader {
    val separator = raw.indexOf(':')
    require(separator > 0) { "Invalid header '$raw'. Expected 'Name: value'." }
    return HttpHeader(
        name = raw.substring(0, separator).trim(),
        value = raw.substring(separator + 1).trimStart(),
    )
}

private fun parseCookie(raw: String): HttpCookie {
    val separator = raw.indexOf('=')
    require(separator > 0) { "Invalid cookie '$raw'. Expected 'name=value'." }
    return HttpCookie(
        name = raw.substring(0, separator).trim(),
        value = raw.substring(separator + 1).trim(),
    )
}
