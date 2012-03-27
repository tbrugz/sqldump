package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

/*
 * XXXxxx (database dependent): DDL: grab contents from procedures, triggers and views 
 * XXXxxx: detach main (SQLDataDump) from data dump
 * TODOne: generate graphml from schema structure
 * TODOne: column type mapping
 * TODOne: FK constraints at end of schema dump script?
 * TODOne: unique constraints? indexes? 
 * TODOne: sequences?
 * XXXdone: include Grants into SchemaModel?
 * TODOne: recursive dump based on FKs
 * XXX(later): usePrecision should be defined by java code (not .properties)
 * XXXdone: dump dbobjects ordered by type (tables, fks, views, triggers, etc(functions, procedures, packages)), name
 * XXXdone: dump different objects to different files (using log4j - different loggers? no!)
 * XXXdone: more flexible output options (option to group or not grants|fks|index with tables - "group" means same file)
 * XXXdone: script output: option to group specific objects (fk, grants, index) with referencing table
 * XXXdone: script output: option to output specific objects (eg FK or Grants) with specific pattern 
 * XXXdone: compact grant syntax
 * TODOne: postgresql/ansi specific features
 * XXXxx: derby specific features?
 * TODOne: grab specific table info (Oracle)
 * TODOne: grab constraints: ~UNIQUE, ~CHECK, xPK, xFK, xNOT NULL ; UNIQUE & CHECK for Oracle!
 * XXXdone: DEFAULT for columns 
 * XXXdone: COMMENT/REMARKS for columns (Oracle) 
 * XXXdone: COMMENT/REMARKS for tables (Oracle)
 * TODOne: bitbucket project's wiki
 * TODOne: main(): args: point to different .properties init files. 
 * XXXdone: Use ${xxx} params inside Properties
 * XXXdone: data dump: limit tables to dump 
 * XXXxx: define output patterns for data dump
 * !TODO: include demo schema and data
 * XXXdone: option to delete initial output dir contents (except special hidden files (unix dotfiles) eg: .svn, .git, .hg)?
 * ---
 * XXXxxx: compare 2 schema models? generate "alter table" database script... see SQLDiff
 * XXX(later): generate schema model from graphML file (XMLUnit?). may be used for model comparison 
 * XXX: new grabber: scriptGrabber - antlr?
 * XXXdone: serialize model (for later comparison)
 * XXXdone: XML schema model grabber/dumper - http://en.wikipedia.org/wiki/XML_data_binding, http://stackoverflow.com/questions/35785/xml-serialization-in-java, http://www.castor.org/xml-framework.html
 *   - x jaxb, xtream, xmlbeans, x castor, jibx
 * XXXdone: new dumper: generate mondrian schema
 * XXXdone: test with sqlite - http://code.google.com/p/sqlite-jdbc/
 * XXX: luciddb?
 * XXX!: new dumper: test case dumper: dumps defined records and its parent/child records based on FKs (needs schema and connection)
 * XXXdone: new dumper: alter schema suggestions (PKs, FKs, "create index"s)
 * XXXdone: fixed prop 'propfilebasedir'/'basepropdir': properties file directory
 * XXX: add shutdown option (Derby). see JDBCSchemaGrabber.grabDbSpecificFeaturesClass()
 * XXX: add startup option, before opening connection (SQLite, ...) - readOnlyConnection , ...
 * ~TODO: sqlregen/sqlrun/sqlexec/sqlcmd // SQLCreate/SQLRecreate/SQLGenerate/SQLRegenerate: command for sending sql statements to database (re-generate database). order for sending statements based on regex
 * XXXxx: default value for 'sqldump.dumpschemapattern'? user? upper(user)/(oracle)? public (postgresql)? only if contained in MetaData().getSchemas()
 * TODO: more transparent way of selecting index grabbing strategy: 'sqldump.dbspecificfeatures.grabindexes' / 'sqldump.doschemadump.indexes'
 * XXX: FK 'on delete cascade'? UNIQUE constraints 'not null'? other modifiers?
 * ~XXX: create view WITH CHECK OPTION - can only update rows thar are accessible through the view (+ WITH READ ONLY)
 * XXX: add junit tests for all "supported" databases (needs sqlregen first?)
 * XXXxx: error dumping blobs
 * XXXxx: add support for blobs (BlobDataDump)
 * XXXxx: add support for cursor in sql (ResultSet as a column type): [x] xml, [x] html, [x] json dumpers
 * XXX: option for queries (or specific queries) to have specific syntax-dumpers
 * XXX: filter tables/executables/trigger (/index/view/mv/sequence ?) by name (include only/exclude)
 * TODO: output ffc with optional trimming
 * TODO: use sql quote when names are equal to sql keywords or have invalid characters (" ", "-", ...) - SchemaModelScriptDumper, AlterSchemaSuggestion
 * XXXdone: move to tbrugz.sqldump.util: IOUtil, Utils, ParametrizedProperties
 * TODOne: add SQLDialectTransformer
 * XXXdone: log4j -> commons logging ( static Log log = LogFactory.getLog(XXX.class) )
 * TODO: sqldump.schemagrab.tables=<schema>.<table>, <table2>
 * TODO: sqldump.schemagrab.xtratables=<schema>.<table>, <table2>
 */
public class SQLDump {
	
	//static/constant properties
	public static final String PROP_PROPFILEBASEDIR = "propfilebasedir"; //"propfiledir" / "propfilebasedir" / "propertiesbasedir" / "basepropdir"
	
	static final String CONN_PROPS_PREFIX = "sqldump";
	
	//sqldump.properties
	static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass";
	static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses";
	static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	static final String PROP_PROCESSINGCLASSES = "sqldump.processingclasses";
	
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	public static final String[] DEFAULT_CLASSLOADING_PACKAGES = { "tbrugz.sqldump", "tbrugz.sqldump.datadump", "tbrugz.sqldump.processors" }; 
	
	public static final String PARAM_PROPERTIES_FILENAME = "-propfile="; 
	public static final String PARAM_USE_SYSPROPERTIES = "-usesysprop"; 
	
	static Log log = LogFactory.getLog(SQLDump.class);
	
	Connection conn;

	Properties papp = new ParametrizedProperties();
	
	public static void init(String[] args, Properties papp) throws Exception {
		log.info("init...");
		//parse args
		String propFilename = PROPERTIES_FILENAME;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
			}
			else if(arg.indexOf(PARAM_USE_SYSPROPERTIES)==0) {
				ParametrizedProperties.setUseSystemProperties(true);
			}
			else {
				log.warn("unrecognized param '"+arg+"'. ignoring...");
			}
		}
		File propFile = new File(propFilename);
		
		//init properties
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		papp.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());

		log.info("loading properties: "+propFile);
		papp.load(new FileInputStream(propFile));
	}

	void init(String[] args) throws Exception {
		SQLDump.init(args, papp);
		
		DBMSResources.instance().setup(papp);
	}

	void end() throws Exception {
		log.info("...done");
		if(conn!=null) {
			conn.close();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {

		SQLDump sdd = new SQLDump();

		try {

		sdd.init(args);
		
		//Utils.showSysProperties();
		
		SchemaModel sm = null;
		SchemaModelGrabber schemaGrabber = null;
		DBMSResources.instance().updateMetaData(null);
		
		//grabbing model
		String grabClassName = sdd.papp.getProperty(PROP_SCHEMAGRAB_GRABCLASS);
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) getClassInstance(grabClassName, DEFAULT_CLASSLOADING_PACKAGES);
			if(schemaGrabber!=null) {
				schemaGrabber.procProperties(sdd.papp);
				if(schemaGrabber.needsConnection() && sdd.conn==null) {
					sdd.conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, sdd.papp);
					DBMSResources.instance().updateMetaData(sdd.conn.getMetaData());
				}
				schemaGrabber.setConnection(sdd.conn);
				sm = schemaGrabber.grabSchema();
			}
			else {
				log.warn("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.warn("no schema grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		
		String dirToDeleteFiles = sdd.papp.getProperty(PROP_DO_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			Utils.deleteDirRegularContents(dirToDeleteFiles);
		}
		
		//FIXME: addLegacyProcessors()
		//sdd.addLegacyProcessors();

		//processing classes
		String processingClassesStr = sdd.papp.getProperty(PROP_PROCESSINGCLASSES);
		if(processingClassesStr!=null) {
			if(sdd.conn==null) {
				sdd.conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, sdd.papp);
				DBMSResources.instance().updateMetaData(sdd.conn.getMetaData());
			}
			String processingClasses[] = processingClassesStr.split(",");
			for(String procClass: processingClasses) {
				AbstractSQLProc sqlproc = (AbstractSQLProc) getClassInstance(procClass.trim(), DEFAULT_CLASSLOADING_PACKAGES);
				if(sqlproc!=null) {
					sqlproc.setProperties(sdd.papp);
					sqlproc.setConnection(sdd.conn);
					sqlproc.setSchemaModel(sm);
					sqlproc.process();
				}
				else {
					log.warn("Error initializing processing class: '"+procClass+"'");
				}
			}
		}
		
		//dumping model
		String dumpSchemaClasses = sdd.papp.getProperty(PROP_SCHEMADUMP_DUMPCLASSES);
		if(dumpSchemaClasses!=null) {
			String dumpClasses[] = dumpSchemaClasses.split(",");
			for(String dumpClass: dumpClasses) {
				SchemaModelDumper schemaDumper = (SchemaModelDumper) getClassInstance(dumpClass.trim(), DEFAULT_CLASSLOADING_PACKAGES);
				if(schemaDumper!=null) {
					schemaDumper.procProperties(sdd.papp);
					schemaDumper.dumpSchema(sm);
				}
				else {
					log.warn("Error initializing dump class: '"+dumpClass+"'");
				}
			}
		}
		else {
			log.warn("no schema dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined");
		}
		
		}
		finally {
			log.info("closing connection: "+sdd.conn);
			sdd.end();
		}
	}
	
	/*
	@Deprecated
	void addLegacyProcessors() {
		if(doTests) {
			papp.setProperty(PROP_PROCESSINGCLASSES, papp.getProperty(PROP_PROCESSINGCLASSES)+","+SQLTests.class.getName());
		}
		if(Utils.getPropBool(papp, PROP_DO_QUERIESDUMP)) {
			papp.setProperty(PROP_PROCESSINGCLASSES, papp.getProperty(PROP_PROCESSINGCLASSES)+","+SQLQueries.class.getName());
		}
	}
	*/
	
	public static Object getClassInstance(String className, String... defaultPackages) {
		Object o = Utils.getClassInstance(className);
		int countPack = 0;
		while(o==null && defaultPackages!=null && defaultPackages.length > countPack) {
			o = Utils.getClassInstance(defaultPackages[countPack]+"."+className);
			countPack++;
		}
		return o;
	}
	
}
