
# sqldump docker images

Docker images loaded with sqldump & some JDBC drivers


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

## running containers

* sqldump

`docker run -it --rm sqldump` - normal execution

* sqlrun

`docker run -it --rm -e TZ=$(</etc/timezone) sqlrun` - normal execution (using host timezone)


* debugging

`docker run -it --rm --entrypoint=/bin/bash sqldump`  
`docker run -it --rm --entrypoint=/bin/bash sqlrun`


## misc

- Timezones in Docker: https://serverfault.com/questions/683605/docker-container-time-timezone-will-not-reflect-changes
