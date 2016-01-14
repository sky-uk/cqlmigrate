# Releasing

We use axion-release which relies on semantic versioning and git tags.

To simply release a bug fix:

    ./gradlew check release
    ./gradlew bintrayUpload

To bump the version (when adding functionality or breaking backwards compatibility):

    ./gradlew release -Prelease.forceVersion=1.0.0
    ./gradlew bintrayUpload

Get the current version:

    ./gradlew currentVersion

Note that the `bintrayUpload` Gradle tasks will fail authentication unless the relevant `openSource*` properties have
been defined in the system gradle.properties or overridden on the Gradle command line.
