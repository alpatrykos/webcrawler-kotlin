package com.patryktargosinski.webcrawler.model

import java.net.URI

enum class JobStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED,
}

data class HttpHeader(
    val name: String,
    val value: String,
)

data class HttpCookie(
    val name: String,
    val value: String,
)

data class CrawlJob(
    val id: Long,
    val seedUrl: String,
    val seedHost: String,
    val allowSubdomains: Boolean,
    val maxDepth: Int,
    val maxPages: Int,
    val delayMs: Long,
    val renderWaitMs: Long,
    val headers: List<HttpHeader>,
    val cookies: List<HttpCookie>,
    val status: JobStatus,
    val createdAt: String,
    val startedAt: String?,
    val finishedAt: String?,
    val errorMessage: String?,
) {
    val seedUri: URI
        get() = URI(seedUrl)
}

data class PageRecord(
    val jobId: Long,
    val normalizedUrl: String,
    val finalUrl: String?,
    val depth: Int,
    val parentUrl: String?,
    val httpStatus: Int?,
    val title: String?,
    val fetchedAt: String,
    val fetchError: String?,
)

data class LinkRecord(
    val jobId: Long,
    val sourceUrl: String,
    val normalizedTargetUrl: String,
    val rawTargetUrl: String,
    val discoveredAt: String,
)

data class JobSummary(
    val id: Long,
    val seedUrl: String,
    val status: JobStatus,
    val createdAt: String,
    val startedAt: String?,
    val finishedAt: String?,
    val errorMessage: String?,
    val pageCount: Int,
    val linkCount: Int,
)

data class CrawlTarget(
    val normalizedUrl: String,
    val depth: Int,
    val parentUrl: String?,
)

data class FetchResult(
    val finalUri: URI,
    val statusCode: Int?,
    val title: String?,
    val html: String,
)
