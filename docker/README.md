
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

`docker run -it --rm sqlrun` - normal execution


* debugging

`docker run -it --rm --entrypoint=/bin/bash sqldump`  
`docker run -it --rm --entrypoint=/bin/bash sqlrun`
