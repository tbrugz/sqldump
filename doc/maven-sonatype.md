
tips for publishing maven artifacts to sonatype
-----------------------------------------------

1- setup gpg

see: http://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/,
http://central.sonatype.org/pages/working-with-pgp-signatures.html


2- setup `~/.m2/settings.xml` ; setup `~/.gpgcredentials`

**settings.xml**: on servers, add sonatype-nexus-snapshots & sonatype-nexus-staging (with users & passwords);

on `$HOME/.gpgcredentials`, put your private key passphrase

see: https://central.sonatype.org/publish/generate-token/


3- upload

* SNAPSHOT: **publish-sonatype-snapshot** - `ant clean test publish-sonatype-snapshot`

* RELEASE: **publish-sonatype-release** - `ant clean test publish-sonatype-release`

(do not forget to `ant resolve` if needed)

3.1- upload sqlmigrate

* SNAPSHOT: (`cd sqlmigrate` && ) `mvn clean deploy`


4- release (if RELEASE version)

Go to <https://oss.sonatype.org/>, select 'staging repositories', select your repository (something like
'comexampleapplication-1010'), review it then 'close' & 'release'  
see: http://central.sonatype.org/pages/releasing-the-deployment.html


troubleshooting
---------------

If an error with 'server redirected too many times' appears, see if you have configured your servers
('sonatype-nexus-snapshots' & 'sonatype-nexus-staging') correctly


links
-----

- Sonatype OSSRH (OSS Repository Hosting) Guide
http://central.sonatype.org/pages/ossrh-guide.html

- OSSRH Guide: Using Ant (and Ivy?)
http://central.sonatype.org/pages/apache-ant.html

- Nexus: Releasing the Deployment
http://central.sonatype.org/pages/releasing-the-deployment.html

- Maven: Password Encryption
https://maven.apache.org/guides/mini/guide-encryption.html

- Where are my GnuPG keys stored?
https://www.enigmail.net/list_archive/2008-March/009003.html

- Open Source Software Nexus Repo
https://oss.sonatype.org/

- Sonatype repos:
-- staging: https://oss.sonatype.org/content/groups/staging/org/bitbucket/tbrugz/sqldump/
-- releases: https://oss.sonatype.org/content/repositories/releases/org/bitbucket/tbrugz/sqldump/


future / ideas
------
http://stackoverflow.com/questions/28071697/is-it-possible-to-pass-a-password-in-maven-deploy-in-the-command-line

setup .mvncredentials

on `$HOME/.mvncredentials`, setup the properties:

	sonatype-nexus-snapshots.username=
	sonatype-nexus-snapshots.password=
	sonatype-nexus-staging.username=
	sonatype-nexus-staging.password=

