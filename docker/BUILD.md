
## building docker images

* sqldump

```shell
ant clean resolve
docker build -t sqldump --file Dockerfile.sqldump .
```

* sqlrun

```shell
ant clean resolve
docker build -t sqlrun --file Dockerfile.sqlrun .
```


## tagging & publishing


```shell
docker login
docker tag sqldump tbrugz/sqldump:0.9.17
docker tag sqldump tbrugz/sqldump:latest
docker tag sqlrun tbrugz/sqlrun:0.9.17
docker tag sqlrun tbrugz/sqlrun:latest
docker push tbrugz/sqldump:0.9.17
docker push tbrugz/sqldump:latest
docker push tbrugz/sqlrun:0.9.17
docker push tbrugz/sqlrun:latest
```


## misc

https://docs.docker.com/docker-hub/

https://stackoverflow.com/questions/36022892/how-to-know-if-docker-is-already-logged-in-to-a-docker-registry-server
