package com.patryktargosinski.webcrawler.util

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import java.net.URI

class UrlPolicyTest {
    @Test
    fun `normalize lowercases host strips fragment and default port`() {
        val normalized = UrlPolicy.normalize("HTTPS://Example.COM:443/path/index.html?x=1#section")

        assertEquals("https://example.com/path/index.html?x=1", normalized.toString())
    }

    @Test
    fun `resolveAndNormalize resolves relative links and removes fragments`() {
        val resolved =
            UrlPolicy.resolveAndNormalize(
                URI("https://example.com/docs/start/index.html"),
                "../guide.html?lang=en#top",
            )

        assertEquals("https://example.com/docs/guide.html?lang=en", resolved.toString())
    }

    @Test
    fun `isInScope only allows subdomains when configured`() {
        val seed = URI("https://example.com/")
        val sameHost = URI("https://example.com/about")
        val subdomain = URI("https://blog.example.com/post")

        assertTrue(UrlPolicy.isInScope(seed, sameHost, allowSubdomains = false))
        assertFalse(UrlPolicy.isInScope(seed, subdomain, allowSubdomains = false))
        assertTrue(UrlPolicy.isInScope(seed, subdomain, allowSubdomains = true))
    }
}
