package com.patryktargosinski.webcrawler.util

import java.net.URI

object UrlPolicy {
    fun normalize(rawUrl: String): URI? {
        val parsed = parse(rawUrl) ?: return null
        return normalize(parsed)
    }

    fun normalize(uri: URI): URI? {
        val scheme = uri.scheme?.lowercase() ?: return null
        if (scheme != "http" && scheme != "https") {
            return null
        }

        val normalized = uri.normalize()
        val host = normalized.host?.lowercase() ?: return null
        val path = normalized.rawPath?.ifBlank { "/" } ?: "/"
        val port = when {
            normalized.port == -1 -> -1
            scheme == "http" && normalized.port == 80 -> -1
            scheme == "https" && normalized.port == 443 -> -1
            else -> normalized.port
        }

        return try {
            URI(
                scheme,
                normalized.rawUserInfo,
                host,
                port,
                path,
                normalized.rawQuery,
                null,
            )
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    fun resolveAndNormalize(baseUri: URI, rawLink: String): URI? {
        val trimmed = rawLink.trim()
        if (trimmed.isEmpty()) {
            return null
        }

        return try {
            normalize(baseUri.resolve(trimmed))
        } catch (_: IllegalArgumentException) {
            null
        }
    }

    fun isInScope(seedUri: URI, candidateUri: URI, allowSubdomains: Boolean): Boolean {
        val seedHost = seedUri.host?.lowercase() ?: return false
        val candidateHost = candidateUri.host?.lowercase() ?: return false

        return candidateHost == seedHost ||
            (allowSubdomains && candidateHost.endsWith(".$seedHost"))
    }

    private fun parse(rawUrl: String): URI? =
        try {
            URI(rawUrl.trim())
        } catch (_: IllegalArgumentException) {
            null
        }
}
