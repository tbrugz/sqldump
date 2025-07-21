
# mavenizing sqldump


## refs

https://maven.apache.org/guides/mini/guide-using-one-source-directory.html

https://maven.apache.org/ref/3.9.9/maven-model/maven.html#build

https://stackoverflow.com/questions/270445/maven-compile-with-multiple-src-directories


## tests

```sh
mvn test -Dtest=tbrugz.sqldiff.SQLDiffMainTest                 # ok when running solo
mvn test -Dtest=tbrugz.sqldump.RoundTripTest#testRoundtrip     # should be ignored
mvn test -Dtest=tbrugz.sqldiff.io.DiffIOTest
```


## comparing two jars

```sh
diff -y <(unzip -l file1.zip) <(unzip -l file2.zip)
diff -y <(unzip -Z1 dist/jars/sqldump.jar | sort) <(unzip -Z1 target/sqldump-0.11-SNAPSHOT.jar | sort)
```

https://unix.stackexchange.com/questions/128303/how-to-list-files-in-a-zip-without-extra-information-in-command-line

https://unix.stackexchange.com/questions/452673/compare-two-zip-files-for-differences

https://diffoscope.org/


## build, install

`mvn clean package`  
`mvn clean install`


## deploy

* SNAPSHOTs:  
`mvn clean deploy`

* RELEASE:
`mvn clean javadoc:jar source:jar deploy -P release`
(publishing may require to go to https://central.sonatype.com/publishing/deployments)


## upgrade version

`mvn versions:set -DnewVersion=<new-version>` - ex: `mvn versions:set -DnewVersion=0.11-SNAPSHOT`


## generate changelog (git)

`git log --pretty=format:"- %s (%ad)%C(yellow)%d%Creset" --date=short --reverse v0.10^^..`


## maven goals & tips

`mvn enforcer:display-info`  
`mvn versions:display-plugin-updates`  
`mvn versions:display-dependency-updates`  
