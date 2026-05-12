
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

check for ignore patterns (`// NOSONAR`) in code...

ref:  
https://docs.sonarsource.com/sonarqube-server/10.8/analyzing-source-code/scanners/sonarscanner-for-maven  
https://docs.sonarsource.com/sonarqube-server/10.8/analyzing-source-code/test-coverage/java-test-coverage  
