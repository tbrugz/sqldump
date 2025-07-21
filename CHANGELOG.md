
sqldump 0.11 [2025-07-xx]
------------
- **java 8+** required (2024-10-30)
- mavenize: **mavenizing sqldump** (2024-11-02)
- mavenize: sqldump: packaging as pom, added modules
- **breaking** moved: sqldump.jar -> **sqldump-core.jar**
- dbmodel: Grant: add 'addColumns'
- build/pom: updated apache poi (xls/xlsx): 3.17 -> 5.3.0
- cdi: CdiUtils: checking if CDI is available (2025-03-13)
- resultset-pivot: performance issue probably fixed
- datadump: xlsx: added suffix '.use-streaming' (2025-07-16)


sqldump 0.10 [2024-10-29]
------------
- **java 7+** required
- java 17 compatibility
- model: Relation: added 'getNamedParameterNames()'
- datadump: added PivotInfo/PivotHeaderRow/PivotHeaderCol
- datadump: xls/xlsx: prettier output
- sqlrun: importers: added '.importfiles.glob' (glob pattern) & '.importfiles.regex'
- **breaking**: sqlrun: '.importfiles' now uses glob pattern
- sqlrun: added '.files.glob' (glob pattern) & '.files.regex' suffixes
- **breaking**: sqlrun: '.files' suffix now uses glob pattern (2021-11-07)
- github: added ant workflow (2021-10-17)
- docker: added image building scripts (2021-12-16)
- util: ConnectionUtil: added DataSourceProvider
- cdi: added (+ CdiUtils) ; util: ConnectionUtil: using CdiUtils
- project: **sqlmigrate**! (2022-06-12)
- project/refactoring: 'sqldump-mondrian' mavenized
- dbmodel: ExecutableObject: added 'deterministic'
- module: sqldump-logback (2023-06-15)
- sqldump/diff: add WhitespaceIgnoreType (2024-10-09)
- dbmodel: Column: added GeneratedInfo


sqldump 0.9.17 [2021-08-29]
--------------
- dbmodel: DBObjectType: added AGGREGATE
- datadump: added MarkdownSyntax (.md)
- dbmodel: Query: added 'namedParameterNames'
- processor: SQLQueries: added '.from-dir' property
- dbmd: DBMSFeatures: added 'sqlExplainPlanQuery()'
- util: ConnectionUtil: added doCommit/doRollback/releaseSavepoint
- dbmodel: added ParametrizedDBObject
- dbmodel: added RemarkableDBObject
- dbmd: DBMSFeatures: added 'sqlLengthFunctionByType' & 'sqlIsNullFunction'
- dbmodel: Column: added type-check methods
- util: ParametrizedProperties: added `_NULL_` placeholder for "coalesced" props
- doc: updated project url (bitbucket -> github) (2020-10-14)
- dbmodel: added ValidatableDBObject
- sqlrun: assert: added `.row@<row-number>.col@<column-name>.eq` checks
- sqlrun: Importer: added importStream()
- sqlrun: tokenizer: added TokenizerUtil - removeSqlComents(), ...
- sqlrun: tokenizer: added TokenizerUtil.getNamedParameters()
- processor: SQLQueries: added named bind parameters
- sqlrun: importer: allowing to skip columns


sqldump 0.9.16 [2018-01-30]
--------------
- dbms: oracle: added prop suffix '.oracle.grab-executble-privileges'
- resultset-pivot: allow "no pivot cols"
- datadump: html: added pivot-awareness
- diff: apply: added '.objecttypes' & '.changetypes' properties (1814)
- dbms: postgresql: added foreign tables support
- datadump: added DumpSyntaxBuilder / HierarchicalDumpSyntax
- datadump: syntax: added 'hasMoreRows' param
- datadump: json: add suffixes '.inner-table.add-data-element', '.inner-table.add-metadata' & '.inner-array-dump-as-array'
- datadump: json: add suffixes '.null-data-element', '.no-array-on-unique-row', '.force-unique-row' (1877)


sqldump 0.9.15 [2017-03-07]
--------------
- dumper: DropScriptDumper: added prop '.ifexists'
- sqlrun: added 'sql' importer (1712)
- datadump: added 'fetchsize' prop suffix
- diff: ResultSetDiff: added 'dumpEquals'
- dbmd: DBMSFeatures: simpler sqlAlterColumnByDiffing()
- processor: added PropertiesLogger
- sqlrun: importer: misc improvements
- dbmd: DBMSFeatures: explainPlan: added params
- dbms: added DB2 support
- dbms: initial CUBRID support
- build/util: Version: added 'build.revisionNumber' & 'build.timestamp' (1751)
- dbmd: DBMSFeatures: added supportsAddColumnAfter() (used by mysql & h2)
- diff2queries: added 'diff2q-defaults.properties'
- dbms: oracle: dump java objects (& added [syntaxfileext]) - issue #1
- dumper: SchemaModelScriptDumper: added [syntaxfileext] pattern


sqldump 0.9.14 [2016-08-03]
--------------
- dbms: added explainPlan()
- util: ParametrizedProperties: accept env vars & added coalesce-like function
- datadump: json: added $metadata , added jsonp/callback
- diff: added parallel grab (ModelGrabber class)
- diff: added prop 'sqldiff.applydiff.<changetype>' - filter diffs to apply by type
- dbms: initial Neo4j support (1524)
- processor: StatsProc: added prop suffix '.stats-by-column'
- datadump: added WebRowSetSingleSyntax / CacheRowSetSyntax
- dbmd: DBMSFeatures: added grabDBMaterializedViews() ; added prop suffix '.grabmaterializedviews'
- resultset2graph: added '.param' & '.nodeparam' properties (1680)
- datadump: added PoiXlsSyntax (xls) & PoiXlsxSyntax (xlsx) - uses apache poi
- sqlrun: added XlsImporter
- sqlrun: added .assert 'processor' (1701)


sqldump 0.9.13 [2015-06-26]
--------------
- util: ConnectionUtil: added suffix '.datasource.contextlookup'
- diff: added prop suffix '.patchfilepattern' / PatchDumper - outputs patch (unified diff)
- def/dbmodel: DBMSFeatures refactoring: added grab views, triggers, executables, sequences & synonyms
- datadiff: multiple syntax support, added HTMLDiff
- diff2queries: added prop 'diff2q.sql'
- sqldump: added prop 'sqldump.schemagrab.metadata'
- datadump: insertinto, xml & html syntaxes changes
- model: grab/dump grants with columns
- model/jaxb: added xml schema
- many other refactorings & fixes


sqldump 0.9.12 [2014-07-03]
--------------
- fixes: changes from checkstyle & findbugs
- model: refactoring: added many getters & setters
- datadump: '.writebom' changes
- datadump: xml: escape properties
- many other small changes/fixes


sqldump 0.9.11 [2014-02-07]
--------------
- dbms: H2: added grabDBTriggers()
- added sqlpivot driver
- pivot: multiple measures, measures in rows or cols
- mdxqueries: add cellset->resultset adapter
- datadump tests & bugfixing
- sqlrun: failonerror by executor
- test: 'grabTrigger" unit test with H2
- jmx: added sqldump & sqlrun props for creating mbean


sqldump 0.9.10 [2013-10-03]
--------------
- sqlrun: importer: more column types
- processors: added SendMail
- sqlrun: importer: added FFCImporter
- removed package deps/cycle:
    - util -> dbmd
    - util -> def (SQLUtils)
- added DiffTwoQueries (diff2q)
- util: removed requirement Class.forName(driverClass) (JDBC 4.0+)
- anttasks fixes
- util: ConnectionUtil: added '.password.base64'
- mondrian: added prop 'snowflake.maxlevel', refactoring & other changes
- properties refactoring: grabber, dumper, ...
- mondrian: option to add hier's lower levels as distinct hierarchy
- mondrian: added caption (cube, dimension, level, hierarchy's 'all-member')


sqldump 0.9.9 [2013-08-30]
-------------
- r2g: initial & final node types
- mondrian: added prop '.measurecolsregex'
- mondrian2graph: added xsl.simple
- datadiff: added prop '.usecommoncolumns'
- model: added Column.ordinalPosition
- fixed: some findbugs/pmd warnings ; ColumnDiff.compareTo()


sqldump 0.9.8 [2013-08-12]
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


sqldump 0.9.7 [2013-07-16]
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


sqldump 0.9.6 [2013-06-18]
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


sqldump 0.9.5 [2013-05-14]
-------------
- JMX monitoring: sqlrun
- mondrian validator
- dmbs
    - firebird support
    - drizzle
- information_schema bugfixes


sqldump 0.9.4 [2013-04-19]
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


sqldump 0.9.3 [2013-04-04]
------------- 
- finish DataDiff: create connection
- change ${} to [] for patterns
    - SQLDiff
    - DataDump
    - SchemaModelScriptDump
    - blob
- cout unit tests, ioutil unit tests
- sqldump version class, version info logging - tbrugz.sqldump.util.Version

