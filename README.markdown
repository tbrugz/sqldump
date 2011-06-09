
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

Usage
-----
- Copy `sqldump.properties.template` to `sqldump.properties`
- Edit `sqldump.properties`
- Run `tbrugz.sqldump.SQLDataDump`
