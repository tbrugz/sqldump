
SQLDump
=======

Utility to dump schema and data from a RDBMS. Features:

- Does schema-dumping, using (mainly) standard java API, by way of `java.sql.DatabaseMetaData`
- Can do data-dumping
- Can be used with any JDBC-compliant databases
- Generates Entity-Relationship diagrams based on Tables and FKs (graphML output)
- Flexible schema output patterns (based on schema name, object type and object name)
- Translation of metadata (column types, ...) between different RDBMS dialects/implementations (partial)

Author: Telmo Brugnara <[tbrugz@gmail.com](mailto:tbrugz@gmail.com)>

License: [AGPLv3](http://www.gnu.org/licenses/agpl.html)


Dependencies
------------
- apache-commons-logging
- log4j
- [kmlutils](https://bitbucket.org/tbrugz/kmlutils) (for graphML output)
- ant (recommended)


Usage (with ant)
----------------
- Run `ant compile`
- Copy `sqldump.properties.template` to `sqldump.properties`
- Edit `sqldump.properties`
- Copy `build.properties.template` to `build.properties`
- Edit `build.properties`
- Run `ant run`


Usage (without ant)
-------------------
- Copy `sqldump.properties.template` to `sqldump.properties`
- Edit `sqldump.properties`
- Compile sources into `bin`
- Run `tbrugz.sqldump.SQLDump`, e.g., "`java -cp bin;lib/kmlutils.jar;lib/commons-logging-1.1.1.jar;lib/log4j-1.2.15.jar;<jdbc-driver-path> tbrugz.sqldump.SQLDump`"
-- you may also specify a different properties file by appending to command line: ` -propfile=<path-to-prop-file>` 
