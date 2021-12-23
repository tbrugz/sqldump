
# sqldump docker images

Docker images loaded with sqldump/sqlrun & some JDBC drivers.

Project repository: https://github.com/tbrugz/sqldump  
Dockerfiles: https://github.com/tbrugz/sqldump/tree/master/docker/


## running containers

* sqldump

`docker run -it --rm tbrugz/sqldump` - normal execution

* sqlrun

`docker run -it --rm -e TZ=$(</etc/timezone) tbrugz/sqlrun` - normal execution (using host timezone)


* debugging (running bash)

`docker run -it --rm --entrypoint=/bin/bash tbrugz/sqldump`  
`docker run -it --rm --entrypoint=/bin/bash tbrugz/sqlrun`


## misc

- Registry Images:  
  https://hub.docker.com/r/tbrugz/sqldump  
  https://hub.docker.com/r/tbrugz/sqlrun

- Timezones in Docker:
  https://serverfault.com/questions/683605/docker-container-time-timezone-will-not-reflect-changes
