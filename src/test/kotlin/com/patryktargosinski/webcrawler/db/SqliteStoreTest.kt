package com.patryktargosinski.webcrawler.db

import com.patryktargosinski.webcrawler.model.HttpCookie
import com.patryktargosinski.webcrawler.model.HttpHeader
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class SqliteStoreTest {
    @Test
    fun `claimNextQueuedJob only returns a job to one worker`() {
        val tempDir = createTempDirectory("sqlite-store-test")
        val dbPath = tempDir.resolve("crawler.db")
        val store = SqliteStore(dbPath)
        val jobId =
            store.enqueueJob(
                seedUrl = "https://example.com/",
                seedHost = "example.com",
                allowSubdomains = false,
                maxDepth = 1,
                maxPages = 10,
                delayMs = 0,
                renderWaitMs = 0,
                headers = listOf(HttpHeader("X-Test", "1")),
                cookies = listOf(HttpCookie("session", "abc")),
            )

        val startLatch = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(2)
        try {
            val futures =
                List(2) {
                    executor.submit<Long?> {
                        startLatch.await(5, TimeUnit.SECONDS)
                        SqliteStore(dbPath).claimNextQueuedJob()?.id
                    }
                }

            startLatch.countDown()
            val claimedIds = futures.mapNotNull { it.get(10, TimeUnit.SECONDS) }

            assertEquals(listOf(jobId), claimedIds.distinct())
            assertEquals(1, claimedIds.size)

            val claimedJob = SqliteStore(dbPath).findJob(jobId)
            assertNotNull(claimedJob)
            assertEquals("RUNNING", claimedJob.status.name)
        } finally {
            executor.shutdownNow()
        }
    }
}
