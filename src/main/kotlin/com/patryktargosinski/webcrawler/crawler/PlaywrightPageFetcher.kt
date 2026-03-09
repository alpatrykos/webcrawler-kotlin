package com.patryktargosinski.webcrawler.crawler

import com.microsoft.playwright.Browser
import com.microsoft.playwright.BrowserType
import com.microsoft.playwright.Page
import com.microsoft.playwright.Playwright
import com.microsoft.playwright.options.Cookie
import com.microsoft.playwright.options.WaitUntilState
import com.patryktargosinski.webcrawler.model.CrawlJob
import com.patryktargosinski.webcrawler.model.FetchResult
import java.net.URI

class PlaywrightPageFetcher(
    private val job: CrawlJob,
    private val navigationTimeoutMs: Double = 30_000.0,
) : AutoCloseable {
    private val playwright = Playwright.create()
    private val browser =
        playwright.chromium().launch(
            BrowserType.LaunchOptions()
                .setHeadless(true),
        )
    private val context =
        browser.newContext(
            Browser.NewContextOptions()
                .setUserAgent(USER_AGENT)
                .setExtraHTTPHeaders(job.headers.associate { it.name to it.value }),
                )

    init {
        if (job.cookies.isNotEmpty()) {
            val seedOrigin = buildSeedOrigin(job)
            context.addCookies(
                job.cookies.map { cookie ->
                    Cookie(cookie.name, cookie.value)
                        .setUrl(seedOrigin)
                },
            )
        }
    }

    fun fetch(uri: URI): FetchResult {
        val page = context.newPage()
        return try {
            val response =
                page.navigate(
                    uri.toString(),
                    Page.NavigateOptions()
                        .setTimeout(navigationTimeoutMs)
                        .setWaitUntil(WaitUntilState.DOMCONTENTLOADED),
                )

            page.waitForTimeout(job.renderWaitMs.toDouble())

            FetchResult(
                finalUri = URI(page.url()),
                statusCode = response?.status(),
                title = page.title().takeIf { it.isNotBlank() },
                html = page.content(),
            )
        } finally {
            page.close()
        }
    }

    override fun close() {
        context.close()
        browser.close()
        playwright.close()
    }

    private fun buildSeedOrigin(job: CrawlJob): String =
        buildString {
            append(job.seedUri.scheme)
            append("://")
            append(job.seedHost)
            if (job.seedUri.port != -1) {
                append(":")
                append(job.seedUri.port)
            }
            append("/")
        }
}
