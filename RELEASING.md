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

Once the artifact has been uploaded to bintray, you will need to create a release on GitHub:
 * Under Releases, click on Draft New Release
 * Select the tag created by the Gradle release task (eg. 1.0.0)
 * The title of the release should be the tag (eg. 1.0.0)
 * In the release notes, include a link to the uploaded artifact on bintray, and list all PRs that have been merged since the last release
 
Don't forget to publish the artifact on bintray!
