
sqldump 0.9.9
-------------
- r2g: initial & final node types
- mondrian: added prop '.measurecolsregex'
- mondrian2graph: added xsl.simple
- datadiff: added prop '.usecommoncolumns'
- model: added Column.ordinalPosition
- fixed: some findbugs/pmd warnings ; ColumnDiff.compareTo()


sqldump 0.9.8
-------------
- LGPLv3 (.md) - https://github.com/tallus/forecast.io.py/blob/master/LICENSE-LGPL.md
- added XML & JSON Serializer
- ~ DBMSResources.addUpdateListener()
- build: added PMD target
- Processor.needsSchemaModel()
- build: added CPD target (& DRY jenkins plugin)
- build/test: added cobertura targets
- build: added findbugs
- sqldiff: add interface DiffDumper & factory
    - ~ SQLDiff refactor: diffgrabbers, diffdumpers, ...
- ~ SQLDiff: unit tests for output, xml & json
- 'sqldiff.applydiff.validate'
    - sqldiff: apply to model (similar to applytosource)
    - option to validate diff before executing (see if diff/patch is compatible)
    - DiffValidator(SchemaModel).validateDiff(Diff) ; .validateDiffs(List<Diff>)
- ~ add ant tasks (sqldump, sqlrun, sqldiff) - should be able to use ant properties - setup();doMain() for sqlrun/dump/diff?
    - parameters: properties, failonerror
    - add Executor interface ?
- identify renames of tables & columns
    - similarity calculator
- tempcolstrategy - see if column is 'not null'... add null column, set data, set not null
- SQLDiff: unit tests for main()
    - apply tests, rename tests
- RS2G: replace ${xxx} inside sql queries
- build: added checkstyle
- mondrian: prop for ignoreMeasureColumnsBelongingToPK - removepkandukfrommeasures
- sqlqueries: prepared statement: getRSMetaData (useful adding columns into model)
- sqldiff: dbid.REPLACE


sqldump 0.9.7
-------------
- change deletefiles to processor
- add deletefiles processor to sqldiff
- build/ivy: process classpath on resolve... ivy.classpath.properties
- dbms: virtuoso
- dbms: sqlserver
- mysqldump-like properties doc/example
- column diff: option to create new (temp) column... (kettle-like)
    - strategy: always,never,whenTypesDiffer,whenNewPrecisionIsSmaller,whenTypesDifferOrNewPrecisionIsSmaller
    - boolean array: always,typeDiffer,newPrecisionSmaller, logical OR?
    - enum?: ALWAYS > NEWPRECISIONSMALLER > TYPESDIFFER > NEVER
- datadump: simple-ods
- datadump: jopendoc-ods - http://www.jopendocument.org/start_spreadsheet_1.html
- ~ sqldiff/run.doapplydiff | patch, .applydifftosource=true|false, .applydiffto=<id>
    - .. i.e.: dump schema diff to database
    - Diff: List<String> getDiff(): 1 'diff' (change) may require many statements...
    - dump datadiff to database
- sqldiff tests: columndiff unit tests
- tests for ResultSet2Graph
- sqldiff to/from json
- ~ test sqldump.jar in command-line! (jaxb serializarion w/ diff, ...)


sqldump 0.9.6
-------------
- datadump: iidb - insert into database
- SMscriptdumper: dump to database
- iidb: ignore errors/fallback to file
- SchemaModelTransformer
- ~ processor: cascading data dump - based on FKs
- ~ refactor SQLDump to preprocess properties & make a list
- JMX monitoring: sqldump
- olap4j util & connector
    - olap4jconnection grabber - processor (updates connection)? Processor.getUpdatedConnection/getNewConnection
- mondrian/olap4j query dumper
- removed mondrian2graphml dep
- xsltprocessor/mondrian2graphml
- test: x cascadingdatadump
- test: ~ modrianschemadump, ~ olap4jmdxqueries
- package sources jar (ivy/mvn)


sqldump 0.9.5
-------------
- JMX monitoring: sqlrun
- mondrian validator
- dmbs
    - firebird support
    - drizzle
- information_schema bugfixes


sqldump 0.9.4
-------------
- ~ DataDump: unit tests for 1 partitionby, many partitionby, 1 dumper, many dumpers
- 'sqldump.failonerror' prop
- FIXME tasks (datadump ok, sqlrun ok)
- jaxb with json - jettison
- ~ throw ProcessingException when error writing to file (sqldump!, run?, diff?) - disk space? permission?
    - ant must also stop with error [if <java failonerror=true>, yes!]
- DataDiff: import .sql (h2 by default?)
- sqldiff xmlin/xmlout - diff as xml
- mariadb: test, doc reference
- ~ monetdb - features


sqldump 0.9.3
------------- 
- finish DataDiff: create connection
- change ${} to [] for patterns
    - SQLDiff
    - DataDump
    - SchemaModelScriptDump
    - blob
- cout unit tests, ioutil unit tests
- sqldump version class, version info logging - tbrugz.sqldump.util.Version
