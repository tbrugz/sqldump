
SQLDump
=======

Utility to dump schema and data from a RDBMS. Features:

- Does schema-dumping, using (mainly) standard java API, by way of `java.sql.DatabaseMetaData`
- Can do data-dumping
- Can be used with any JDBC-compliant databases
- Generates Entity-Relationship diagrams based on Tables and FKs (graphML output - [yEd](http://www.yworks.com/products/yed/) recommended)
- Flexible schema output patterns (based on schema name, object type and object name)
- Translation of metadata (column types, ...) between different RDBMS dialects/implementations (partial)

Author: Telmo Brugnara <[tbrugz@gmail.com](mailto:tbrugz@gmail.com)>

License: [AGPLv3](http://www.gnu.org/licenses/agpl.html)


Dependencies
------------
- apache-commons-logging
- log4j
- [kmlutils](https://bitbucket.org/tbrugz/kmlutils) (for graphML output)
- database-dependent JDBC jars (e.g. 
	[PostgreSQL](http://jdbc.postgresql.org/download.html), 
	[MySQL](http://dev.mysql.com/downloads/connector/j/5.0.html), 
	[Oracle](http://www.oracle.com/technetwork/database/features/jdbc/index-091264.html),
	[Derby](http://db.apache.org/derby/derby_downloads.html),
	[HSQLDB](http://hsqldb.org/))
- ant (recommended)


Usage (with sources & ant)
--------------------------
- Run `ant compile`
- Copy `sqldump.properties.template` to `sqldump.properties`
- Edit `sqldump.properties`
- Copy `build.properties.template` to `build.properties`
- Edit `build.properties`
- Run `ant run`


Usage (with sources, without ant)
---------------------------------
- Copy `sqldump.properties.template` to `sqldump.properties`
- Edit `sqldump.properties`
- Compile sources into `bin`
- Run `tbrugz.sqldump.SQLDump`, e.g., "`java -cp bin;lib/kmlutils.jar;lib/commons-logging-1.1.1.jar;lib/log4j-1.2.15.jar;<jdbc-driver-path> tbrugz.sqldump.SQLDump`"

(you may also specify a different properties file by appending to command line: ` -propfile=<path-to-prop-file>`) 


Usage (without sources)
-----------------------
- Download `sqldump.jar` jar from [sqldump/downloads](https://bitbucket.org/tbrugz/sqldump/downloads)
- Download all jars from [sqldump/lib](https://bitbucket.org/tbrugz/sqldump/src/tip/lib/) (hint: download all into a `lib` subfolder)
- (windows) Download latest version of [sqldump.bat](https://bitbucket.org/tbrugz/sqldump/raw/tip/sqldump.bat.template) template as `sqldump.bat`
- Download latest version of [sqldump.properties](https://bitbucket.org/tbrugz/sqldump/raw/tip/sqldump.properties.template) template as `sqldump.properties`
- Download jdbc jars for your database of choice
- Edit `sqldump.properties` and (windows) `sqldump.bat`
- (windows) Run `sqldump.bat`

(you may also specify a different properties file by appending to command line inside `sqldump.bat`: ` -propfile=<path-to-prop-file>`) 

- (unix-like) Run `tbrugz.sqldump.SQLDump`, e.g., "`java -cp sqldump.jar:lib/kmlutils.jar:lib/commons-logging-1.1.1.jar:lib/log4j-1.2.15.jar:<jdbc-driver-path> tbrugz.sqldump.SQLDump`"

(you may also specify a different properties file by appending to command line: ` -propfile=<path-to-prop-file>`) 
