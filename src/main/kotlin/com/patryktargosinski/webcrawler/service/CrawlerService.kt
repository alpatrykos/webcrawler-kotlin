package com.patryktargosinski.webcrawler.service

import com.patryktargosinski.webcrawler.db.SqliteStore
import com.patryktargosinski.webcrawler.model.CrawlJob
import com.patryktargosinski.webcrawler.model.HttpCookie
import com.patryktargosinski.webcrawler.model.HttpHeader
import com.patryktargosinski.webcrawler.model.JobStatus
import com.patryktargosinski.webcrawler.model.JobSummary
import com.patryktargosinski.webcrawler.util.UrlPolicy
import org.slf4j.LoggerFactory
import java.nio.file.Path

class CrawlerService(
    dbPath: Path,
    private val store: SqliteStore = SqliteStore(dbPath),
    private val workerFactory: (SqliteStore) -> CrawlWorker = { CrawlWorker(it) },
) {
    private val logger = LoggerFactory.getLogger(CrawlerService::class.java)

    fun enqueueJob(
        seedUrl: String,
        maxDepth: Int,
        maxPages: Int,
        delayMs: Long,
        renderWaitMs: Long,
        allowSubdomains: Boolean,
        headers: List<HttpHeader>,
        cookies: List<HttpCookie>,
    ): Long {
        require(maxDepth >= 0) { "maxDepth must be >= 0" }
        require(maxPages > 0) { "maxPages must be > 0" }
        require(delayMs >= 0) { "delayMs must be >= 0" }
        require(renderWaitMs >= 0) { "renderWaitMs must be >= 0" }

        val normalizedSeed = UrlPolicy.normalize(seedUrl)
            ?: throw IllegalArgumentException("seedUrl must be a valid HTTP(S) URL")

        return store.enqueueJob(
            seedUrl = normalizedSeed.toString(),
            seedHost = normalizedSeed.host,
            allowSubdomains = allowSubdomains,
            maxDepth = maxDepth,
            maxPages = maxPages,
            delayMs = delayMs,
            renderWaitMs = renderWaitMs,
            headers = headers,
            cookies = cookies,
        )
    }

    fun runNext(): CrawlJob? {
        store.failRunningJobsAsStale()
        val job = store.claimNextQueuedJob() ?: return null
        runClaimedJob(job)
        return store.findJob(job.id)
    }

    fun runAll(): List<CrawlJob> {
        store.failRunningJobsAsStale()
        val finishedJobs = mutableListOf<CrawlJob>()

        while (true) {
            val job = store.claimNextQueuedJob() ?: break
            runClaimedJob(job)
            store.findJob(job.id)?.let(finishedJobs::add)
        }

        return finishedJobs
    }

    fun listStatus(jobId: Long? = null): List<JobSummary> = store.listJobSummaries(jobId)

    fun store(): SqliteStore = store

    private fun runClaimedJob(job: CrawlJob) {
        try {
            workerFactory(store).run(job)
            store.markJobCompleted(job.id)
        } catch (exception: FatalCrawlException) {
            logger.warn("Crawl job {} failed", job.id, exception)
            store.markJobFailed(job.id, exception.message ?: "Crawler failed")
        } catch (exception: Exception) {
            logger.error("Unexpected crawl failure for job {}", job.id, exception)
            store.markJobFailed(job.id, exception.message ?: "Unexpected crawl failure")
        }
    }
}

fun formatStatus(summaries: List<JobSummary>): String {
    if (summaries.isEmpty()) {
        return "No crawl jobs found."
    }

    return summaries.joinToString(separator = "\n\n") { summary ->
        buildString {
            appendLine("Job ${summary.id}")
            appendLine("  status: ${summary.status.name}")
            appendLine("  seed: ${summary.seedUrl}")
            appendLine("  pages: ${summary.pageCount}")
            appendLine("  links: ${summary.linkCount}")
            appendLine("  created: ${summary.createdAt}")
            summary.startedAt?.let { appendLine("  started: $it") }
            summary.finishedAt?.let { appendLine("  finished: $it") }
            if (summary.status == JobStatus.FAILED && summary.errorMessage != null) {
                append("  error: ${summary.errorMessage}")
            }
        }.trimEnd()
    }
}
