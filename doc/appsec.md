
semgrep
=======

running semgrep:

```sh
semgrep --config=auto --exclude-rule java.lang.security.audit.formatted-sql-string.formatted-sql-string --exclude-rule java.lang.security.audit.sqli.jdbc-sqli.jdbc-sqli
```

check for ignore patterns (`// nosemgrep`) in code...


sonar/sonarqube
=====

```sh
export SONAR_TOKEN=<TOKEN>
mvn sonar:sonar -Dsonar.host.url=http://localhost:9000
```

other parameters:
- `sonar.projectKey=sqldump`
- `sonar.projectName=SqlDump`
- `sonar.login=<TOKEN>`

check for ignore patterns (`// NOSONAR`) in code...

ref:  
https://docs.sonarsource.com/sonarqube-server/10.8/analyzing-source-code/scanners/sonarscanner-for-maven  
https://docs.sonarsource.com/sonarqube-server/10.8/analyzing-source-code/test-coverage/java-test-coverage  



github codeql
=====

see: `.github/workflows/codeql.yml`  
https://github.com/tbrugz/sqldump/security/code-scanning  
https://github.com/tbrugz/sqldump/security/code-scanning/tools/CodeQL/status  


---
---


github dependabot
=====

https://github.com/tbrugz/sqldump/security/dependabot

see: `.github/dependabot.yml`

