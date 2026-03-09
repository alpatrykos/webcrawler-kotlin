plugins {
    kotlin("jvm") version "2.2.21"
    application
}

group = "com.patryktargosinski"
version = "0.1.0"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(17)
}

dependencies {
    implementation("com.github.ajalt.clikt:clikt:4.4.0")
    implementation("com.microsoft.playwright:playwright:1.43.0")
    implementation("org.jsoup:jsoup:1.18.1")
    implementation("org.xerial:sqlite-jdbc:3.45.2.0")
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}

application {
    mainClass.set("com.patryktargosinski.webcrawler.MainKt")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    dependsOn("installPlaywrightBrowser")
}

tasks.register<JavaExec>("installPlaywrightBrowser") {
    group = "application"
    description = "Installs the Playwright Chromium browser required by the crawler."
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("com.microsoft.playwright.CLI")
    args("install", "chromium")
}
