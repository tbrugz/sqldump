
## demo: sqldump-generic

To execute sqldump, set env properties, like:

(DB Url - Linux)

```sh
export SQLD_JDBC_URL="jdbc:h2:mem:testdb"
export SQLD_JDBC_URL="jdbc:derby://localhost:1527/testdb;create=true"
export SQLD_JDBC_URL="jdbc:derby://localhost:1527/testdb"
export SQLD_JDBC_URL="jdbc:hive2://localhost:10000/default"
```

(... Windows)

```bat
set SQLD_JDBC_URL=jdbc:h2:mem:testdb
set SQLD_JDBC_URL=jdbc:derby://localhost:1527/testdb;create=true
set SQLD_JDBC_URL=jdbc:hive2://localhost:10000/default
```

(driver class - possibly optional)

```bat
set SQLD_DRIVER=org.apache.derby.jdbc.ClientDriver
set SQLD_DRIVER=org.apache.hive.jdbc.HiveDriver
```

(etc)
```bat
set SQLD_SCHEMAS=queryon,default
```

Then run:

```sh
mvn package exec:java
```

<!--
`mvn package exec:java -Dexec.args="XXX"`
-->


## launching databases

### Apache Derby

```sh
docker run --rm -p 1527:1527 --name derby-db az82/docker-derby
```

https://github.com/az82/docker-derby
https://hub.docker.com/r/az82/docker-derby/


### Apache Hive

```sh
export HIVE_VERSION="4.1.0"
#set HIVE_VERSION=4.1.0
docker run --rm -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:${HIVE_VERSION}
```
