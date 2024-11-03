
# mavenizing sqldump


## refs

https://maven.apache.org/guides/mini/guide-using-one-source-directory.html

https://maven.apache.org/ref/3.9.9/maven-model/maven.html#build

https://stackoverflow.com/questions/270445/maven-compile-with-multiple-src-directories


## comparing two jars

```sh
diff -y <(unzip -l file1.zip) <(unzip -l file2.zip)
diff -y <(unzip -Z1 dist/jars/sqldump.jar | sort) <(unzip -Z1 target/sqldump-0.11-SNAPSHOT.jar | sort)
```

https://unix.stackexchange.com/questions/128303/how-to-list-files-in-a-zip-without-extra-information-in-command-line

https://unix.stackexchange.com/questions/452673/compare-two-zip-files-for-differences

https://diffoscope.org/
