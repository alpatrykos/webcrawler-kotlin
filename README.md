# Kotlin Web Crawler CLI

Local CLI crawler for single-host crawl jobs backed by SQLite and rendered through Playwright Chromium.

## Requirements

- JDK 17

## Setup

Install the Playwright browser once before running the crawler or the test suite:

```bash
./gradlew installPlaywrightBrowser
```

## Commands

```bash
./gradlew run --args="enqueue --seed-url https://example.com"
./gradlew run --args="run-next"
./gradlew run --args="run-all"
./gradlew run --args="status"
```
