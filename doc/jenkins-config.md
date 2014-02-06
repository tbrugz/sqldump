
sqldump in jenkins
------------------

you may create a sqldump job in your jenkins instance easily, just follow these steps

steps
-----

1. install the following plugins:
    * Mercurial
    * Checkstyle
    * FindBugs
    * PMD
    * DRY
    * Task Scanner
    * Cobertura

2. run [jenkins-cli](https://wiki.jenkins-ci.org/display/JENKINS/Jenkins+CLI):
  `java -jar jenkins-cli.jar -s http://[your-server] create-job sqldump < [sqldump-repo]/doc/jenkins-config.xml`

3. [optional] at **jenkins > sqldump > configure**: change your Mercurial Repository URL to your `[sqldump-repo]`

