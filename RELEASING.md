# Releasing

## Set up a Sonatype Account

1. Create a Sonatype Account using your `@sky.uk` email address: https://issues.sonatype.org/secure/Signup!default.jspa
2. Create a Jira issue requesting access to the `uk.sky` group id: https://issues.sonatype.org/secure/CreateIssue.jspa?issuetype=11003&pid=10134

## Generate a GPG Signing Key

1. Install GPG

Red Hat / CentOS:
```shell
yum install gnupg
```

Ubuntu / Debian:
```shell
apt-get install gnupg
```

Mac OS X
```shell
brew install gnupg
```

2. Generate a signing key, selecting option 1 (RSA and RSA) for the kind of key you want:

```shell
gpg --full-generate-key
```

3. Export your key:

```shell
gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
```

5. Take note of the last 8 characters of the key, for example if this was the output:

```shell
pub   rsa3072 2021-11-16 [SC]
      5E323D1244C346B15FBF73A210F0219E5BEFCDB2
uid                      John Smith <john.smith@sky.uk>
sub   rsa3072 2021-11-16 [E]
```

Take note of `5BEFCDB2`. This is your signing key ID and needs to be provided when uploading to Maven Central.

6. Distribute your public key:

```shell
gpg --keyserver keyserver.ubuntu.com --send-keys 5E323D1244C346B15FBF73A210F0219E5BEFCDB2
```

More info on the above steps can be found here https://central.sonatype.org/publish/requirements/gpg/.

## Bump the version

We use axion-release which relies on semantic versioning and git tags.

:warning: Warning, the below commands should only be run on the master branch.

Get the current version:
```shell
./gradlew currentVersion
```

To simply release a bug fix and update the patch version run:
```shell
./gradlew check release
```

To bump the minor or major version (when adding functionality or breaking backwards compatibility):
```shell
./gradlew check release -Prelease.forceVersion=1.0.0
```

## Upload to Maven Central

:warning: Warning, once published it's impossible to delete or update the artifact. See here for more info 
https://central.sonatype.org/faq/can-i-change-a-component/.

1. To upload all artifacts to Maven Central run:

```shell
./gradlew uploadArchives -PossrhUsername=your-jira-id -PossrhPassword=your-jira-password -Psigning.keyId=YourKeyId -Psigning.password=YourPublicKeyPassword -Psigning.secretKeyRingFile=PathToYourKeyRingFile
```

- `signing.password` should be set to the password you used when generating your signing key
- `signing.secretKeyRingFile` should be the path to your `secring.gpg` file e.g. `~/.gnupg/secring.gpg`
- The above properties can alternatively be stored in your systems `~/.gradle/gradle.properties` file

2. Login to https://s01.oss.sonatype.org/ using the Sonatype account you created earlier
3. Click `Staging Repositories` in the left-hand sidebar
4. Find the version you wish to publish and review the details
5. Click `Close` in the top bar menu
6. If Sonatype's automated checks are happy that the artifact meets the
[requirements](https://central.sonatype.org/publish/requirements/) you should be able to click `Release` in the top bar
menu
7. Once the artifact has been uploaded to Maven Central, you will need to create a release on GitHub:  
7.1. Under Releases, click on Draft New Release  
7.2. The title of the release should be the tag (eg. 1.0.0)  
7.3. In the release notes, include a link to the uploaded artifact on Maven Central, and list all PRs that have been merged since the last release

More info on the above steps can be found here https://central.sonatype.org/publish/release/.
