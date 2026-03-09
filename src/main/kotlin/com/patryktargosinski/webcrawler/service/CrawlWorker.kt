package com.patryktargosinski.webcrawler.service

import com.patryktargosinski.webcrawler.crawler.PlaywrightPageFetcher
import com.patryktargosinski.webcrawler.crawler.RobotsPolicyLoader
import com.patryktargosinski.webcrawler.db.SqliteStore
import com.patryktargosinski.webcrawler.model.CrawlJob
import com.patryktargosinski.webcrawler.model.CrawlTarget
import com.patryktargosinski.webcrawler.model.LinkRecord
import com.patryktargosinski.webcrawler.model.PageRecord
import com.patryktargosinski.webcrawler.util.UrlPolicy
import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.Clock
import java.time.Duration
import java.time.Instant

class CrawlWorker(
    private val store: SqliteStore,
    private val robotsPolicyLoader: RobotsPolicyLoader = RobotsPolicyLoader(),
    private val clock: Clock = Clock.systemUTC(),
    private val sleeper: (Long) -> Unit = Thread::sleep,
) {
    private val logger = LoggerFactory.getLogger(CrawlWorker::class.java)

    fun run(job: CrawlJob) {
        val robotsPolicy = robotsPolicyLoader.load(job.seedUri)
        if (!robotsPolicy.isAllowed(job.seedUri)) {
            throw FatalCrawlException("Seed URL is disallowed by robots.txt")
        }

        val queue = ArrayDeque<CrawlTarget>()
        val seenUrls = linkedSetOf<String>()
        val normalizedSeed = UrlPolicy.normalize(job.seedUri)?.toString()
            ?: throw FatalCrawlException("Seed URL is not a valid HTTP(S) URL")

        queue += CrawlTarget(normalizedSeed, depth = 0, parentUrl = null)
        seenUrls += normalizedSeed

        var pageCount = 0
        var lastFetchAt: Instant? = null

        PlaywrightPageFetcher(job).use { fetcher ->
            while (queue.isNotEmpty() && pageCount < job.maxPages) {
                val target = queue.removeFirst()
                val targetUri = URI(target.normalizedUrl)

                if (!robotsPolicy.isAllowed(targetUri)) {
                    continue
                }

                applyRateLimit(job.delayMs, lastFetchAt)

                val fetchedAt = Instant.now(clock).toString()
                try {
                    val result = fetcher.fetch(targetUri)
                    lastFetchAt = Instant.now(clock)

                    store.upsertPage(
                        PageRecord(
                            jobId = job.id,
                            normalizedUrl = target.normalizedUrl,
                            finalUrl = result.finalUri.toString(),
                            depth = target.depth,
                            parentUrl = target.parentUrl,
                            httpStatus = result.statusCode,
                            title = result.title,
                            fetchedAt = fetchedAt,
                            fetchError = null,
                        ),
                    )

                    UrlPolicy.normalize(result.finalUri)?.toString()?.let(seenUrls::add)
                    pageCount += 1

                    if (target.depth >= job.maxDepth) {
                        continue
                    }

                    val document = Jsoup.parse(result.html, result.finalUri.toString())
                    document.select("a[href]").forEach { anchor ->
                        val rawTarget = anchor.attr("href")
                        val normalizedTarget = UrlPolicy.resolveAndNormalize(result.finalUri, rawTarget) ?: return@forEach
                        val normalizedTargetUrl = normalizedTarget.toString()

                        store.insertLink(
                            LinkRecord(
                                jobId = job.id,
                                sourceUrl = target.normalizedUrl,
                                normalizedTargetUrl = normalizedTargetUrl,
                                rawTargetUrl = rawTarget,
                                discoveredAt = Instant.now(clock).toString(),
                            ),
                        )

                        if (!UrlPolicy.isInScope(job.seedUri, normalizedTarget, job.allowSubdomains)) {
                            return@forEach
                        }
                        if (!robotsPolicy.isAllowed(normalizedTarget)) {
                            return@forEach
                        }
                        if (!seenUrls.add(normalizedTargetUrl)) {
                            return@forEach
                        }

                        queue +=
                            CrawlTarget(
                                normalizedUrl = normalizedTargetUrl,
                                depth = target.depth + 1,
                                parentUrl = target.normalizedUrl,
                            )
                    }
                } catch (exception: Exception) {
                    lastFetchAt = Instant.now(clock)
                    logger.warn("Failed to fetch {}", target.normalizedUrl, exception)
                    store.upsertPage(
                        PageRecord(
                            jobId = job.id,
                            normalizedUrl = target.normalizedUrl,
                            finalUrl = null,
                            depth = target.depth,
                            parentUrl = target.parentUrl,
                            httpStatus = null,
                            title = null,
                            fetchedAt = fetchedAt,
                            fetchError = exception.message ?: exception::class.simpleName ?: "Unknown error",
                        ),
                    )
                    pageCount += 1
                }
            }
        }
    }

    private fun applyRateLimit(delayMs: Long, lastFetchAt: Instant?) {
        if (delayMs <= 0 || lastFetchAt == null) {
            return
        }

        val elapsed = Duration.between(lastFetchAt, Instant.now(clock)).toMillis()
        if (elapsed < delayMs) {
            sleeper(delayMs - elapsed)
        }
    }
}

class FatalCrawlException(message: String) : RuntimeException(message)
