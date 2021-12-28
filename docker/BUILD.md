
## building docker images


`ant buildfull`

**OR**

```shell
ant clean resolve
docker build -t sqldump --file Dockerfile.sqldump .
docker build -t sqlrun --file Dockerfile.sqlrun .
```


## tagging & publishing


* Login

`docker login`


* Tag & Publish


`ant publish`

**OR**

```shell
#export TAG=0.9.17
# or export TAG=0.10-SNAPSHOT
# or export TAG=latest
docker tag sqldump tbrugz/sqldump:$TAG
docker tag sqlrun tbrugz/sqlrun:$TAG
docker push tbrugz/sqldump:$TAG
docker push tbrugz/sqlrun:$TAG
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
