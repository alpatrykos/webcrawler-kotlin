package com.patryktargosinski.webcrawler.crawler

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

class RobotsPolicy private constructor(private val rules: List<RobotsRule>) {
    fun isAllowed(uri: URI): Boolean {
        if (rules.isEmpty()) {
            return true
        }

        val candidate = buildString {
            append(uri.rawPath?.ifBlank { "/" } ?: "/")
            uri.rawQuery?.let { append("?").append(it) }
        }

        val bestRule =
            rules
                .filter { it.matches(candidate) }
                .maxWithOrNull(compareBy<RobotsRule> { it.patternLength }.thenBy { if (it.allow) 1 else 0 })

        return bestRule?.allow ?: true
    }

    companion object {
        fun allowAll(): RobotsPolicy = RobotsPolicy(emptyList())

        fun parse(body: String, userAgent: String): RobotsPolicy {
            val groups = mutableListOf<RobotsGroup>()
            var currentGroup: RobotsGroup? = null
            var collectingRules = false

            body.lineSequence().forEach { rawLine ->
                val line = rawLine.substringBefore('#').trim()
                if (line.isEmpty()) {
                    return@forEach
                }

                val separator = line.indexOf(':')
                if (separator < 0) {
                    return@forEach
                }

                val directive = line.substring(0, separator).trim().lowercase()
                val value = line.substring(separator + 1).trim()

                when (directive) {
                    "user-agent" -> {
                        if (currentGroup == null || collectingRules) {
                            currentGroup = RobotsGroup()
                            groups += currentGroup!!
                        }
                        currentGroup!!.agents += value.lowercase()
                        collectingRules = false
                    }

                    "allow", "disallow" -> {
                        if (currentGroup == null || value.isEmpty()) {
                            return@forEach
                        }
                        currentGroup!!.rules += RobotsRule.from(value, directive == "allow")
                        collectingRules = true
                    }
                }
            }

            val agentKey = userAgent.lowercase()
            val exactMatches = groups.filter { group -> group.agents.any { it == agentKey } }
            val selectedGroups = if (exactMatches.isNotEmpty()) exactMatches else groups.filter { "*" in it.agents }
            return RobotsPolicy(selectedGroups.flatMap { it.rules })
        }
    }
}

class RobotsPolicyLoader(
    private val httpClient: HttpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build(),
) {
    fun load(seedUri: URI): RobotsPolicy {
        val robotsUri =
            URI(
                seedUri.scheme,
                null,
                seedUri.host,
                seedUri.port,
                "/robots.txt",
                null,
                null,
            )

        val request =
            HttpRequest.newBuilder(robotsUri)
                .timeout(Duration.ofSeconds(10))
                .header("User-Agent", USER_AGENT)
                .GET()
                .build()

        return try {
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                RobotsPolicy.parse(response.body(), USER_AGENT)
            } else {
                RobotsPolicy.allowAll()
            }
        } catch (_: Exception) {
            RobotsPolicy.allowAll()
        }
    }
}

private data class RobotsGroup(
    val agents: MutableList<String> = mutableListOf(),
    val rules: MutableList<RobotsRule> = mutableListOf(),
)

private data class RobotsRule(
    val allow: Boolean,
    private val regex: Regex,
    val patternLength: Int,
) {
    fun matches(candidate: String): Boolean = regex.containsMatchIn(candidate)

    companion object {
        fun from(pattern: String, allow: Boolean): RobotsRule {
            val endsAtSuffix = pattern.endsWith("$")
            val content = if (endsAtSuffix) pattern.dropLast(1) else pattern
            val escaped = Regex.escape(content).replace("\\*", ".*")
            val regexBody = buildString {
                append("^")
                append(escaped)
                if (endsAtSuffix) {
                    append("$")
                }
            }
            return RobotsRule(
                allow = allow,
                regex = Regex(regexBody),
                patternLength = content.length,
            )
        }
    }
}

const val USER_AGENT = "KotlinWebCrawler/1.0"
