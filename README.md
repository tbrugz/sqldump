
SQLDump
=======

[![GNU Lesser General Public License, v3](https://img.shields.io/github/license/tbrugz/sqldump.svg?label=License&color=blue)](LICENSE)
[![Maven Central](https://img.shields.io/maven-central/v/org.bitbucket.tbrugz/sqldump.svg?label=Maven%20Central)](https://search.maven.org/artifact/org.bitbucket.tbrugz/sqldump)
[![CI](https://github.com/tbrugz/sqldump/actions/workflows/ant.yml/badge.svg)](https://github.com/tbrugz/sqldump/actions/workflows/ant.yml)


Utility to dump schema and data from a RDBMS. Features:

- Does schema-dumping, using (mainly) standard java API, by way of `java.sql.DatabaseMetaData`
- Does data-dumping (formats: csv, xml, html, json, sql 'insert into', sql 'update by PK', fixed column size, blob)
- Can be used with any JDBC-compliant databases
- Generates Entity-Relationship diagrams based on Tables and FKs (graphML output - [yEd](http://www.yworks.com/products/yed/) recommended)
- Flexible schema output patterns (based on schema name, object type and object name)
- Translation of metadata (column types) between different RDBMS dialects/implementations (partial)

SQLDump also has three subprojects:

- **SQLRun**: runs SQL scripts from files - may be used for regenerating database from dumped SQL-scripts. 
	See [sqlrun.template.properties](sqlrun.template.properties) for more info 
- **SQLDiff**: generates a diff from 2 schema models.
	See [sqldiff.template.properties](sqldiff.template.properties) for more info 
- **SQLMigrate**: tool for schema migration.
	See [sqlmigrate/README.md](sqlmigrate/README.md) and [sqlmigrate.template.properties](sqlmigrate/doc/sqlmigrate.template.properties) for more info
- **Diff2Queries**: generates data diff from 2 SQL queries.
	See [diff2q.template.properties](diff2q.template.properties) for more info 

Author: Telmo Brugnara <[tbrugz@gmail.com](mailto:tbrugz@gmail.com)>

License: [LGPLv3](https://www.gnu.org/licenses/lgpl-3.0.html) - see [LICENSE](LICENSE)


SQLDump - Basic Process
-----------------------

SQLDump processing consists of:

1. 1 **Grabber** (implementation of `SchemaModelGrabber`, grabs a `SchemaModel`)
2. 'n' **Processors** (implementation of `Processor`, usually uses a `Connection` or `SchemaModel`)
  & **Dumpers** (implementation of `SchemaModelDumper`, dumps a `SchemaModel`)

*Grabber* can be:

- `JDBCSchemaGrabber` - Grabs schema metadata from a JDBC connection 
- `JAXBSchemaXMLSerializer` - Grabs schema metadata from a XML file 
- `JSONSchemaSerializer` - Grabs schema metadata from a JSON file 

*Processors* can be:

- `DataDump` - Dumps data based on grabbed schema (can partition data from 1 table in different files, can dump in different formats)
- `SQLQueries` - Dumps data based on SQL-queries (same as `DataDump` - for each query)
- `CascadingDataDump` - Dumps data based on table relationships (FKs), given initial tables/filters
- `graph.ResultSet2GraphML` - Dumps a graphML diagram based on a SQL-query
- `SQLDialectTransformer` - Transforms schema models between different sql-dialects
- `SQLRunProcessor` - Loads properties into SQLRun and execute statements & importers
- `mondrianschema.Olap4jMDXQueries` - Dumps data from olap4j/mondrian engine
- `mondrianschema.MondrianSchema2GraphProcessor` - Dumps a graphML diagram based on a Mondrian Schema file
- `mondrianschema.MondrianSchemaValidator` - Validates a mondrian schema
- `xtraproc.StatsProc` - Grabs statistics from database

*Dumpers* can be:

- `SchemaModelScriptDumper` - Dumps schema model in SQL-script format (DDL)
- `JAXBSchemaXMLSerializer` - Dumps a XML representation of the schema model
- `JSONSchemaSerializer` - Dumps a JSON representation of the schema model
- `graph.Schema2GraphML` - Generates a Entity-Relationship diagram based on schema model
- `mondrianschema.MondrianSchemaDumper` - Generates a Star/Snowflake [Mondrian Schema](http://mondrian.pentaho.com/) based on schema model
- `xtradumpers.AlterSchemaSuggester` - Generates suggestions of SQL-scripts for altering the schema model (beta)
- `xtradumpers.DropScriptDumper` - Generates drop SQL-scripts

All processing is controlled by a properties file. See [sqldump.template.properties](sqldump.template.properties)
for more info.

Usage examples can be found at [doc/examples](doc/examples).


Dependencies
------------
- java 7 or newer
- [apache-commons-logging](http://commons.apache.org/logging/)
- [log4j](http://logging.apache.org/log4j/1.2/) - optional but recommended
- database-dependent JDBC jars ( e.g.
	[PostgreSQL](http://jdbc.postgresql.org/download.html),
	[MySQL](http://dev.mysql.com/downloads/connector/j/5.0.html),
	[Oracle](http://www.oracle.com/technetwork/database/features/jdbc/index-091264.html),
	[Derby](http://db.apache.org/derby/derby_downloads.html),
	[HSQLDB](http://hsqldb.org/),
	[H2](http://www.h2database.com/),
	[SQLite](http://code.google.com/p/sqlite-jdbc/),
	[MariaDB](https://downloads.mariadb.org/client-java/),
	[Drizzle](http://www.drizzle.org/content/download), 
	[MonetDB](http://dev.monetdb.org/downloads/Java/Latest/),
	[Firebird](http://jaybirdwiki.firebirdsql.org/),
	[Virtuoso](http://docs.openlinksw.com/virtuoso/VirtuosoDriverJDBC.html),
	[jTDS/SQLServer](http://jtds.sourceforge.net/),
	[Neo4j](https://github.com/neo4j-contrib/neo4j-jdbc)
	)
- [kmlutils](https://github.com/tbrugz/kmlutils) - optional, for graphML output
- [jettison](http://jettison.codehaus.org/) - optional, for JSON output
- [olap4j](http://www.olap4j.org/) & [mondrian](http://mondrian.pentaho.com/) - optional, for Mondrian Schema output, validation & data dump
- [ant](http://ant.apache.org/) - recommended for building
- [ivy](http://ant.apache.org/ivy/) - recommended for building


Building from sources (with ant & ivy)
--------------------------------------
- Run `git clone https://github.com/tbrugz/sqldump <project-dir>` (if not done already)
- Run `ant prepare`
- Install Ivy (`mkdir -p $HOME/.ant/lib` + `curl -o $HOME/.ant/lib/ivy-2.5.0.jar https://repo1.maven.org/maven2/org/apache/ivy/ivy/2.5.0/ivy-2.5.0.jar`)
  or `ant ivy-install` (if not done already)
- (*obsolete* - see `ivy-install`) Add to project dir an `ivysettings.xml` file that points to the [sqldump maven repo](https://bitbucket.org/tbrugz/mvn-repo)
  (like [this](https://bitbucket.org/tbrugz/mvn-repo/raw/master/ivysettings.xml) ; better: `cp templates/ivysettings.xml ivysettings.xml`)
- (*obsolete* - see `ivy-install`) Copy `templates/build.properties` to `build.properties`
- (*optional*) Edit `build.properties`
- (*optional/eclipse*) Use [IvyDE](https://ant.apache.org/ivy/ivyde/), import project, right click + `Ivy > Resolve`
- Run `ant resolve`
- (*optional*) `ant test`
- Run `ant dist` or `ant publish` (publishes, by default, to local maven repo: `$HOME/.m2/repository`) or `ant all`
- (*optional*) Publish maven artifacts: Install Maven Ant tasks
  (`curl -o $HOME/.ant/lib/maven-ant-tasks-2.1.3.jar https://repo1.maven.org/maven2/org/apache/maven/maven-ant-tasks/2.1.3/maven-ant-tasks-2.1.3.jar`)
  & `ant publish-mvn-files`


Running (with sources)
----------------------
- Download jdbc jars for your database of choice
- Edit `sqldump.properties`
- Run `ant run` *or*
- Run `tbrugz.sqldump.SQLDump`, e.g., `java -cp bin;lib/kmlutils.jar;lib/commons-logging-1.1.1.jar;lib/log4j-1.2.15.jar;<jdbc-driver-path> tbrugz.sqldump.SQLDump <options>`


Not building? Setup env (without sources)
-----------------------------------------
- Download `sqldump.jar` jar from [sqldump maven repo](https://bitbucket.org/tbrugz/mvn-repo/src/tip/org/bitbucket/tbrugz/sqldump)
  (e.g.: [sqldump 0.9.16](https://bitbucket.org/tbrugz/mvn-repo/src/tip/org/bitbucket/tbrugz/sqldump/0.9.16/sqldump-0.9.16.jar))
- Download jar dependencies, especially *apache-commons-logging*, to `lib` (may be downloaded from [sqldump/downloads](https://bitbucket.org/tbrugz/sqldump/downloads))
- (windows) Download [sqldump.bat.template](sqldump.bat.template) as `sqldump.bat`
  or (unix-like) download [sqldump.sh.template](sqldump.sh.template) as `sqldump.sh`
- Download latest version of [sqldump.template.properties](sqldump.template.properties) as `sqldump.properties`
- Edit `sqldump.properties` and (windows) `sqldump.bat` or (unix-like) `sqldump.sh` (you may include command-line options at end)


Running (without sources)
-------------------------
- Download jdbc jars for your database of choice
- (windows) Run `sqldump.bat`
- (unix-like) Run `sqldump.sh` or run `tbrugz.sqldump.SQLDump`, e.g., `java -cp sqldump.jar:lib/kmlutils.jar:lib/commons-logging-1.1.1.jar:lib/log4j-1.2.15.jar:<jdbc-driver-path> tbrugz.sqldump.SQLDump <options>`


Building maven 'modules'
------------------------
- `ant mvn-modules-install` (sqlmigrate & sqldump-mondrian modules)


Building or running with Docker
------------------------------
- See [docker/README.md](docker/README.md) or [docker/BUILD.md](docker/BUILD.md)


Command-line options
--------------------
- `-propfile=<path-to-prop-file>`: loads a different config properties file
- `-propresource=<path-to-resource>`: loads a different config properties resource
- `-D<property>[=<value>]`: define property with value
- `-usesysprop=[true|false]`: loads system properties besides the config file properties (default is true)
- `--help`: show help and exit
- `--version`: show version and exit


Artifact repositories
---------------------

**Releases:**  
- [maven central](https://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22org.bitbucket.tbrugz%22%20AND%20a%3A%22sqldump%22)

**Snapshots:**  
- [sonatype snapshots](https://oss.sonatype.org/content/repositories/snapshots/org/bitbucket/tbrugz/sqldump/)
  - see [how to configure settings.xml](https://stackoverflow.com/a/7717234/616413)

**Maven dependency config:**

	<dependency>
		<groupId>org.bitbucket.tbrugz</groupId>
		<artifactId>sqldump</artifactId>
		<version>0.9.17</version>
	</dependency>


Publishing
----------
- To publish on maven central, see [doc/maven-sonatype.md](doc/maven-sonatype.md)
- To check code quality on sonarcloud, see [doc/sonarqube.md](doc/sonarqube.md)


Misc/End notes
--------------
To build with [Jenkins](http://jenkins-ci.org/), see [doc/jenkins-config.md](doc/jenkins-config.md)

To use with Eclipse, [IvyDE](https://ant.apache.org/ivy/ivyde/download.html) is recommended
