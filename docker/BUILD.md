
## building docker images


`ant buildfull`

**OR**

```shell
ant clean resolve
docker build -t sqldump --file Dockerfile.sqldump .
docker build -t sqlrun --file Dockerfile.sqlrun .
docker build -t sqldiff --file Dockerfile.sqldiff .
docker build -t diff2queries --file Dockerfile.diff2queries .
docker build -t sqlmigrate --file Dockerfile.sqlmigrate .
```


## tagging & publishing


* Login

`docker login`


* Tag & Publish


`ant publish` (using TAG from `../src/sqldump-version.properties`) ; `ant publish -DTAG=<tag>` (`ant publish -DTAG=latest`)

**OR**

```shell
#export TAG=0.10
# or export TAG=0.11-SNAPSHOT
# or export TAG=latest
docker tag sqldump tbrugz/sqldump:$TAG
docker tag sqlrun tbrugz/sqlrun:$TAG
docker tag sqldiff tbrugz/sqldiff:$TAG
docker tag diff2queries tbrugz/diff2queries:$TAG
docker tag sqlmigrate tbrugz/sqlmigrate:$TAG

docker push tbrugz/sqldump:$TAG
docker push tbrugz/sqlrun:$TAG
docker push tbrugz/sqldiff:$TAG
docker push tbrugz/diff2queries:$TAG
docker push tbrugz/sqlmigrate:$TAG
```

* Publish README

```shell
docker pushrm --file README.md tbrugz/sqldump
docker pushrm --file README.md tbrugz/sqlrun
```


## misc

- https://docs.docker.com/docker-hub/

- pushrm - https://github.com/christian-korneck/docker-pushrm

- https://stackoverflow.com/questions/36022892/how-to-know-if-docker-is-already-logged-in-to-a-docker-registry-server
