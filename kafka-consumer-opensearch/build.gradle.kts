plugins {
    id("java")
}

group = "org.conductor.demos"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
    implementation("org.slf4j:slf4j-api:2.0.13")

    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    implementation("org.slf4j:slf4j-simple:2.0.13")

    // https://search.maven.org/artifact/org.opensearch.client/opensearch-rest-high-level-client/1.2.4/jar
    implementation ("org.opensearch.client:opensearch-rest-high-level-client:1.3.2")

    // https://search.maven.org/artifact/com.google.code.gson/gson/2.9.0/jar
    implementation ("com.google.code.gson:gson:2.9.0")
}

tasks.test {
    useJUnitPlatform()
}