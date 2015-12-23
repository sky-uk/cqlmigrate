# Releasing

We use axion-release which relies on semantic versioning and git tags.

To simply release a bug fix:

    ./gradlew check release
    ./gradlew publishArtifactPublicationToOpenSourceRepository

To bump the version (when adding functionality or breaking backwards compatibility):

    ./gradlew release -Prelease.forceVersion=1.0.0
    ./gradlew publishArtifactPublicationToOpenSourceRepository

Get the current version:

    ./gradlew currentVersion

Note that none of the above Gradle tasks will work correctly unless the relevant `openSource*` properties have been
defined in the system gradle.properties.
