
tips for publishing maven artifacts to sonatype
-----------------------------------------------

1- setup gpg

see: http://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/,
http://central.sonatype.org/pages/working-with-pgp-signatures.html


2- setup .m2/settings.xml ; setup .gpgcredentials

**settings.xml**: on servers, add sonatype-nexus-snapshots & sonatype-nexus-staging (with users & passwords);

on `$HOME/.gpgcredentials`, put your private key passphrase


3- upload

`ant publish-sonatype-release`


4- release

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

