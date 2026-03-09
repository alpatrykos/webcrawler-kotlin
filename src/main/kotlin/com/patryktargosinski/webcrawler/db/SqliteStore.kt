package com.patryktargosinski.webcrawler.db

import com.patryktargosinski.webcrawler.model.CrawlJob
import com.patryktargosinski.webcrawler.model.HttpCookie
import com.patryktargosinski.webcrawler.model.HttpHeader
import com.patryktargosinski.webcrawler.model.JobStatus
import com.patryktargosinski.webcrawler.model.JobSummary
import com.patryktargosinski.webcrawler.model.LinkRecord
import com.patryktargosinski.webcrawler.model.PageRecord
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant

class SqliteStore(private val dbPath: Path) {
    init {
        Class.forName("org.sqlite.JDBC")
        dbPath.toAbsolutePath().parent?.let(Files::createDirectories)
        bootstrap()
    }

    fun bootstrap() {
        connect().use { connection ->
            listOf(
                """
                CREATE TABLE IF NOT EXISTS crawl_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    seed_url TEXT NOT NULL,
                    seed_host TEXT NOT NULL,
                    allow_subdomains INTEGER NOT NULL,
                    max_depth INTEGER NOT NULL,
                    max_pages INTEGER NOT NULL,
                    delay_ms INTEGER NOT NULL,
                    render_wait_ms INTEGER NOT NULL,
                    headers_text TEXT NOT NULL DEFAULT '',
                    cookies_text TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    error_message TEXT
                )
                """.trimIndent(),
                """
                CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status_created
                ON crawl_jobs(status, created_at, id)
                """.trimIndent(),
                """
                CREATE TABLE IF NOT EXISTS pages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    normalized_url TEXT NOT NULL,
                    final_url TEXT,
                    depth INTEGER NOT NULL,
                    parent_url TEXT,
                    http_status INTEGER,
                    title TEXT,
                    fetched_at TEXT NOT NULL,
                    fetch_error TEXT,
                    UNIQUE(job_id, normalized_url),
                    FOREIGN KEY(job_id) REFERENCES crawl_jobs(id) ON DELETE CASCADE
                )
                """.trimIndent(),
                """
                CREATE INDEX IF NOT EXISTS idx_pages_job_depth
                ON pages(job_id, depth, normalized_url)
                """.trimIndent(),
                """
                CREATE TABLE IF NOT EXISTS links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER NOT NULL,
                    source_url TEXT NOT NULL,
                    normalized_target_url TEXT NOT NULL,
                    raw_target_url TEXT NOT NULL,
                    discovered_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES crawl_jobs(id) ON DELETE CASCADE
                )
                """.trimIndent(),
                """
                CREATE INDEX IF NOT EXISTS idx_links_job_source
                ON links(job_id, source_url)
                """.trimIndent(),
            ).forEach { statement ->
                connection.createStatement().use { it.execute(statement) }
            }
        }
    }

    fun enqueueJob(
        seedUrl: String,
        seedHost: String,
        allowSubdomains: Boolean,
        maxDepth: Int,
        maxPages: Int,
        delayMs: Long,
        renderWaitMs: Long,
        headers: List<HttpHeader>,
        cookies: List<HttpCookie>,
    ): Long {
        val now = Instant.now().toString()
        val sql =
            """
            INSERT INTO crawl_jobs (
                seed_url,
                seed_host,
                allow_subdomains,
                max_depth,
                max_pages,
                delay_ms,
                render_wait_ms,
                headers_text,
                cookies_text,
                status,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()

        return connect().use { connection ->
            connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS).use { statement ->
                statement.setString(1, seedUrl)
                statement.setString(2, seedHost)
                statement.setInt(3, if (allowSubdomains) 1 else 0)
                statement.setInt(4, maxDepth)
                statement.setInt(5, maxPages)
                statement.setLong(6, delayMs)
                statement.setLong(7, renderWaitMs)
                statement.setString(8, serializeHeaders(headers))
                statement.setString(9, serializeCookies(cookies))
                statement.setString(10, JobStatus.QUEUED.name)
                statement.setString(11, now)
                statement.executeUpdate()

                statement.generatedKeys.use { keys ->
                    if (keys.next()) {
                        keys.getLong(1)
                    } else {
                        error("Failed to create crawl job")
                    }
                }
            }
        }
    }

    fun failRunningJobsAsStale(message: String = "Marked stale by worker startup"): Int {
        val sql =
            """
            UPDATE crawl_jobs
            SET status = ?, finished_at = ?, error_message = ?
            WHERE status = ?
            """.trimIndent()
        val now = Instant.now().toString()

        return connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, JobStatus.FAILED.name)
                statement.setString(2, now)
                statement.setString(3, message)
                statement.setString(4, JobStatus.RUNNING.name)
                statement.executeUpdate()
            }
        }
    }

    fun claimNextQueuedJob(): CrawlJob? {
        val claimedAt = Instant.now().toString()

        return connect().use { connection ->
            try {
                connection.createStatement().use { it.execute("BEGIN IMMEDIATE TRANSACTION") }

                val jobId =
                    connection.prepareStatement(
                        """
                        SELECT id
                        FROM crawl_jobs
                        WHERE status = ?
                        ORDER BY created_at ASC, id ASC
                        LIMIT 1
                        """.trimIndent(),
                    ).use { statement ->
                        statement.setString(1, JobStatus.QUEUED.name)
                        statement.executeQuery().use { resultSet ->
                            if (resultSet.next()) {
                                resultSet.getLong("id")
                            } else {
                                null
                            }
                        }
                    }

                if (jobId == null) {
                    connection.createStatement().use { it.execute("COMMIT") }
                    return@use null
                }

                val updated =
                    connection.prepareStatement(
                        """
                        UPDATE crawl_jobs
                        SET status = ?, started_at = ?, finished_at = NULL, error_message = NULL
                        WHERE id = ? AND status = ?
                        """.trimIndent(),
                    ).use { statement ->
                        statement.setString(1, JobStatus.RUNNING.name)
                        statement.setString(2, claimedAt)
                        statement.setLong(3, jobId)
                        statement.setString(4, JobStatus.QUEUED.name)
                        statement.executeUpdate()
                    }

                val job = if (updated == 1) loadJob(connection, jobId) else null
                connection.createStatement().use { it.execute("COMMIT") }
                job
            } catch (exception: Exception) {
                connection.createStatement().use { it.execute("ROLLBACK") }
                throw exception
            }
        }
    }

    fun markJobCompleted(jobId: Long) {
        updateJobStatus(jobId, JobStatus.COMPLETED, null)
    }

    fun markJobFailed(jobId: Long, errorMessage: String) {
        updateJobStatus(jobId, JobStatus.FAILED, errorMessage)
    }

    fun upsertPage(record: PageRecord) {
        val sql =
            """
            INSERT INTO pages (
                job_id,
                normalized_url,
                final_url,
                depth,
                parent_url,
                http_status,
                title,
                fetched_at,
                fetch_error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id, normalized_url) DO UPDATE SET
                final_url = excluded.final_url,
                depth = excluded.depth,
                parent_url = excluded.parent_url,
                http_status = excluded.http_status,
                title = excluded.title,
                fetched_at = excluded.fetched_at,
                fetch_error = excluded.fetch_error
            """.trimIndent()

        connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setLong(1, record.jobId)
                statement.setString(2, record.normalizedUrl)
                statement.setNullableString(3, record.finalUrl)
                statement.setInt(4, record.depth)
                statement.setNullableString(5, record.parentUrl)
                statement.setNullableInt(6, record.httpStatus)
                statement.setNullableString(7, record.title)
                statement.setString(8, record.fetchedAt)
                statement.setNullableString(9, record.fetchError)
                statement.executeUpdate()
            }
        }
    }

    fun insertLink(record: LinkRecord) {
        val sql =
            """
            INSERT INTO links (
                job_id,
                source_url,
                normalized_target_url,
                raw_target_url,
                discovered_at
            ) VALUES (?, ?, ?, ?, ?)
            """.trimIndent()

        connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setLong(1, record.jobId)
                statement.setString(2, record.sourceUrl)
                statement.setString(3, record.normalizedTargetUrl)
                statement.setString(4, record.rawTargetUrl)
                statement.setString(5, record.discoveredAt)
                statement.executeUpdate()
            }
        }
    }

    fun findJob(jobId: Long): CrawlJob? =
        connect().use { connection ->
            loadJob(connection, jobId)
        }

    fun listJobSummaries(jobId: Long? = null): List<JobSummary> {
        val sql =
            buildString {
                append(
                    """
                    SELECT
                        j.id,
                        j.seed_url,
                        j.status,
                        j.created_at,
                        j.started_at,
                        j.finished_at,
                        j.error_message,
                        COALESCE((SELECT COUNT(*) FROM pages p WHERE p.job_id = j.id), 0) AS page_count,
                        COALESCE((SELECT COUNT(*) FROM links l WHERE l.job_id = j.id), 0) AS link_count
                    FROM crawl_jobs j
                    """.trimIndent(),
                )
                if (jobId != null) {
                    append(" WHERE j.id = ?")
                }
                append(" ORDER BY j.id ASC")
            }

        return connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                if (jobId != null) {
                    statement.setLong(1, jobId)
                }
                statement.executeQuery().use { resultSet ->
                    buildList {
                        while (resultSet.next()) {
                            add(
                                JobSummary(
                                    id = resultSet.getLong("id"),
                                    seedUrl = resultSet.getString("seed_url"),
                                    status = JobStatus.valueOf(resultSet.getString("status")),
                                    createdAt = resultSet.getString("created_at"),
                                    startedAt = resultSet.getString("started_at"),
                                    finishedAt = resultSet.getString("finished_at"),
                                    errorMessage = resultSet.getString("error_message"),
                                    pageCount = resultSet.getInt("page_count"),
                                    linkCount = resultSet.getInt("link_count"),
                                ),
                            )
                        }
                    }
                }
            }
        }
    }

    fun listPages(jobId: Long): List<PageRecord> {
        val sql =
            """
            SELECT
                job_id,
                normalized_url,
                final_url,
                depth,
                parent_url,
                http_status,
                title,
                fetched_at,
                fetch_error
            FROM pages
            WHERE job_id = ?
            ORDER BY depth ASC, normalized_url ASC
            """.trimIndent()

        return connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setLong(1, jobId)
                statement.executeQuery().use { resultSet ->
                    buildList {
                        while (resultSet.next()) {
                            add(
                                PageRecord(
                                    jobId = resultSet.getLong("job_id"),
                                    normalizedUrl = resultSet.getString("normalized_url"),
                                    finalUrl = resultSet.getString("final_url"),
                                    depth = resultSet.getInt("depth"),
                                    parentUrl = resultSet.getString("parent_url"),
                                    httpStatus = resultSet.getNullableInt("http_status"),
                                    title = resultSet.getString("title"),
                                    fetchedAt = resultSet.getString("fetched_at"),
                                    fetchError = resultSet.getString("fetch_error"),
                                ),
                            )
                        }
                    }
                }
            }
        }
    }

    fun listLinks(jobId: Long): List<LinkRecord> {
        val sql =
            """
            SELECT
                job_id,
                source_url,
                normalized_target_url,
                raw_target_url,
                discovered_at
            FROM links
            WHERE job_id = ?
            ORDER BY source_url ASC, normalized_target_url ASC, id ASC
            """.trimIndent()

        return connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setLong(1, jobId)
                statement.executeQuery().use { resultSet ->
                    buildList {
                        while (resultSet.next()) {
                            add(
                                LinkRecord(
                                    jobId = resultSet.getLong("job_id"),
                                    sourceUrl = resultSet.getString("source_url"),
                                    normalizedTargetUrl = resultSet.getString("normalized_target_url"),
                                    rawTargetUrl = resultSet.getString("raw_target_url"),
                                    discoveredAt = resultSet.getString("discovered_at"),
                                ),
                            )
                        }
                    }
                }
            }
        }
    }

    private fun updateJobStatus(jobId: Long, status: JobStatus, errorMessage: String?) {
        val sql =
            """
            UPDATE crawl_jobs
            SET status = ?, finished_at = ?, error_message = ?
            WHERE id = ?
            """.trimIndent()
        val now = Instant.now().toString()

        connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.setString(1, status.name)
                statement.setString(2, now)
                statement.setNullableString(3, errorMessage)
                statement.setLong(4, jobId)
                statement.executeUpdate()
            }
        }
    }

    private fun loadJob(connection: Connection, jobId: Long): CrawlJob? {
        val sql =
            """
            SELECT
                id,
                seed_url,
                seed_host,
                allow_subdomains,
                max_depth,
                max_pages,
                delay_ms,
                render_wait_ms,
                headers_text,
                cookies_text,
                status,
                created_at,
                started_at,
                finished_at,
                error_message
            FROM crawl_jobs
            WHERE id = ?
            """.trimIndent()

        return connection.prepareStatement(sql).use { statement ->
            statement.setLong(1, jobId)
            statement.executeQuery().use { resultSet ->
                if (resultSet.next()) {
                    toJob(resultSet)
                } else {
                    null
                }
            }
        }
    }

    private fun connect(): Connection {
        val connection = DriverManager.getConnection("jdbc:sqlite:${dbPath.toAbsolutePath()}")
        connection.createStatement().use {
            it.execute("PRAGMA foreign_keys = ON")
            it.execute("PRAGMA busy_timeout = 5000")
            it.execute("PRAGMA journal_mode = WAL")
        }
        return connection
    }

    private fun toJob(resultSet: ResultSet): CrawlJob =
        CrawlJob(
            id = resultSet.getLong("id"),
            seedUrl = resultSet.getString("seed_url"),
            seedHost = resultSet.getString("seed_host"),
            allowSubdomains = resultSet.getInt("allow_subdomains") == 1,
            maxDepth = resultSet.getInt("max_depth"),
            maxPages = resultSet.getInt("max_pages"),
            delayMs = resultSet.getLong("delay_ms"),
            renderWaitMs = resultSet.getLong("render_wait_ms"),
            headers = deserializeHeaders(resultSet.getString("headers_text")),
            cookies = deserializeCookies(resultSet.getString("cookies_text")),
            status = JobStatus.valueOf(resultSet.getString("status")),
            createdAt = resultSet.getString("created_at"),
            startedAt = resultSet.getString("started_at"),
            finishedAt = resultSet.getString("finished_at"),
            errorMessage = resultSet.getString("error_message"),
        )

    private fun serializeHeaders(headers: List<HttpHeader>): String =
        headers.joinToString("\n") { "${it.name}: ${it.value}" }

    private fun deserializeHeaders(raw: String): List<HttpHeader> =
        raw.lineSequence()
            .map(String::trim)
            .filter(String::isNotEmpty)
            .map { line ->
                val separator = line.indexOf(':')
                HttpHeader(
                    name = line.substring(0, separator).trim(),
                    value = line.substring(separator + 1).trimStart(),
                )
            }.toList()

    private fun serializeCookies(cookies: List<HttpCookie>): String =
        cookies.joinToString("\n") { "${it.name}=${it.value}" }

    private fun deserializeCookies(raw: String): List<HttpCookie> =
        raw.lineSequence()
            .map(String::trim)
            .filter(String::isNotEmpty)
            .map { line ->
                val separator = line.indexOf('=')
                HttpCookie(
                    name = line.substring(0, separator).trim(),
                    value = line.substring(separator + 1).trim(),
                )
            }.toList()
}

private fun PreparedStatement.setNullableString(index: Int, value: String?) {
    if (value == null) {
        setNull(index, Types.VARCHAR)
    } else {
        setString(index, value)
    }
}

private fun PreparedStatement.setNullableInt(index: Int, value: Int?) {
    if (value == null) {
        setNull(index, Types.INTEGER)
    } else {
        setInt(index, value)
    }
}

private fun ResultSet.getNullableInt(column: String): Int? {
    val value = getInt(column)
    return if (wasNull()) null else value
}
