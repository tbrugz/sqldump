package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;
import tbrugz.sqldump.util.Version;

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
 * XXX: option for queries to have specific syntax-dumpers
 * XXXdone: option for specific queries to have specific syntax-dumpers
 * XXX: filter tables/executables/trigger (/index/view/mv/sequence ?) by name (include only/exclude)
 * TODO: output ffc with optional trimming
 * TODO: use sql quote when names are equal to sql keywords or have invalid characters (" ", "-", ...) - SchemaModelScriptDumper, AlterSchemaSuggestion
 * XXXdone: move to tbrugz.sqldump.util: IOUtil, Utils, ParametrizedProperties
 * TODOne: add SQLDialectTransformer
 * XXXdone: log4j -> commons logging ( static Log log = LogFactory.getLog(XXX.class) )
 * TODO: sqldump.schemagrab.tables=<schema>.<table>, <table2>
 * TODO: sqldump.schemagrab.xtratables=<schema>.<table>, <table2>
 * TODO: warnings: grabber with no dumper or processor || dumper with no grabber || no dumper, no grabber, no processor
 */
public class SQLDump {
	
	//static/constant properties
	public static final String PROP_PROPFILEBASEDIR = "propfilebasedir"; //"propfiledir" / "propfilebasedir" / "propertiesbasedir" / "basepropdir"
	
	static final String CONN_PROPS_PREFIX = "sqldump";
	
	//sqldump.properties
	public static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass";
	public static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses";
	public static final String PROP_PROCESSINGCLASSES = "sqldump.processingclasses";
	public static final String PROP_PROCESSINGCLASSES_AFTERDUMPERS = "sqldump.processingclasses.afterdumpers";

	static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	static final String PROP_CONNPROPPREFIX = "sqldump.connpropprefix";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	public static final String[] DEFAULT_CLASSLOADING_PACKAGES = { "tbrugz.sqldump", "tbrugz.sqldump.datadump", "tbrugz.sqldump.processors" }; 
	
	//cli parameters
	public static final String PARAM_PROPERTIES_FILENAME = "-propfile=";
	public static final String PARAM_PROPERTIES_RESOURCE = "-propresource=";
	public static final String PARAM_USE_SYSPROPERTIES = "-usesysprop=";
	
	static final Log log = LogFactory.getLog(SQLDump.class);
	
	Connection conn;

	final Properties papp = new ParametrizedProperties();
	
	//XXX: move to utils(?)... (used by sqldump & sqlrun -- why not sqldiff?) 
	public static void init(String[] args, Properties papp) throws IOException {
		log.info("init... [version "+Version.getVersion()+"]");
		boolean useSysPropSetted = false;
		boolean propFilenameSetted = false;
		boolean propResourceSetted = false;
		//parse args
		String propFilename = PROPERTIES_FILENAME;
		String propResource = null;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
				propFilenameSetted = true;
			}
			else if(arg.indexOf(PARAM_PROPERTIES_RESOURCE)==0) {
				propResource = arg.substring(PARAM_PROPERTIES_RESOURCE.length());
				propResourceSetted = true;
			}
			else if(arg.indexOf(PARAM_USE_SYSPROPERTIES)==0) {
				String useSysProp = arg.substring(PARAM_USE_SYSPROPERTIES.length());
				ParametrizedProperties.setUseSystemProperties(useSysProp.equalsIgnoreCase("true"));
				useSysPropSetted = true;
			}
			else {
				log.warn("unrecognized param '"+arg+"'. ignoring...");
			}
		}
		if(!useSysPropSetted) {
			ParametrizedProperties.setUseSystemProperties(true); //set to true by default
			useSysPropSetted = true;
		}
		log.debug("using sys properties: "+ParametrizedProperties.isUseSystemProperties());
		
		InputStream propIS = null;
		if(propResourceSetted) {
			//XXX: set PROP_PROPFILEBASEDIR for resources?
			
			log.info("loading properties resource: "+propResource);
			propIS = SQLDump.class.getResourceAsStream(propResource);
		}
		else {
			File propFile = new File(propFilename);
			
			//init properties
			File propFileDir = propFile.getAbsoluteFile().getParentFile();
			log.debug("propfile base dir: "+propFileDir);
			papp.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
			
			if(propFile.exists()) {
				log.info("loading properties: "+propFile);
				propIS = new FileInputStream(propFile);
			}
			else {
				log.info("properties file '"+propFile+"' does not exist");
			}
		}
		try {
			if(propIS!=null) {
				papp.load(propIS);
			}
		}
		catch(FileNotFoundException e) {
			if(propResourceSetted) {
				log.warn("prop resource not found: "+propResource);
			}
			else if(propFilenameSetted) {
				log.warn("prop file not found: "+propFilename);
			}
		}
		/*catch(IOException e) {
			if(propResourceSetted) {
				log.warn("error loading prop resource: "+propResource);
			}
			else if(propFilenameSetted) {
				log.warn("error loading prop file: "+propFilename);
			}
		}*/
		
		ColTypeUtil.setProperties(papp);
	}

	void init(String[] args) throws IOException {
		SQLDump.init(args, papp);
		
		DBMSResources.instance().setup(papp);
	}

	void end(boolean closeConnection) throws SQLException {
		if(closeConnection) {
			SQLUtils.ConnectionUtil.closeConnection(conn);
		}
		log.info("...done");
	}
	
	/**
	 * @param args
	 * @throws NamingException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDump sdd = new SQLDump();
		sdd.doMain(args, null, null);
	}

	public void doMain(String[] args, Properties prop) throws ClassNotFoundException, SQLException, NamingException, IOException {
		doMain(args, prop, null);
	}
	
	public void doMain(String[] args, Properties prop, Connection c) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDump sdd = this;
		if(c!=null) { sdd.conn = c; }
		
		try {

		sdd.init(args);
		
		if(prop!=null) {
			sdd.papp.putAll(prop);
		}
		
		//Utils.showSysProperties();
		
		SchemaModel sm = null;
		SchemaModelGrabber schemaGrabber = null;
		//DBMSResources.instance().updateMetaData(null);
		
		//class names
		String grabClassName = sdd.papp.getProperty(PROP_SCHEMAGRAB_GRABCLASS);
		String processingClassesStr = sdd.papp.getProperty(PROP_PROCESSINGCLASSES);
		String processingClassesAfterDumpersStr = sdd.papp.getProperty(PROP_PROCESSINGCLASSES_AFTERDUMPERS);
		String dumpSchemaClasses = sdd.papp.getProperty(PROP_SCHEMADUMP_DUMPCLASSES);
		
		if(grabClassName!=null && dumpSchemaClasses==null) {
			log.warn("grabber class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined but no dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined");
		}
		if(grabClassName==null && dumpSchemaClasses!=null) {
			log.warn("dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined but no grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		if(grabClassName==null && dumpSchemaClasses==null && processingClassesStr==null) {
			log.warn("no grabber [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'], dumper [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] or processing [prop '"+PROP_PROCESSINGCLASSES+"'] classes defined");
		}
		
		//grabbing model
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) Utils.getClassInstance(grabClassName, DEFAULT_CLASSLOADING_PACKAGES);
			if(schemaGrabber!=null) {
				schemaGrabber.procProperties(sdd.papp);
				if(schemaGrabber.needsConnection()) {
					if(sdd.conn==null) { sdd.setupConnection(); }
					schemaGrabber.setConnection(sdd.conn);
				}
				sm = schemaGrabber.grabSchema();
				if(sm!=null) {
					DBMSResources.instance().updateDbId(sm.getSqlDialect());
				}
				else {
					log.warn("no model grabbed!");
				}
			}
			else {
				log.error("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.debug("no schema grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		
		String dirToDeleteFiles = sdd.papp.getProperty(PROP_DO_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			Utils.deleteDirRegularContents(dirToDeleteFiles);
		}
		
		//inits DBMSFeatures if not already initted
		DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass(); //XXX: really needed?
		log.debug("DBMSFeatures: "+feats);
		
		//processing classes
		if(processingClassesStr!=null) {
			sdd.processClasses(processingClassesStr, sm);
		}
		
		//dumping model
		if(dumpSchemaClasses!=null) {
			String dumpClasses[] = dumpSchemaClasses.split(",");
			for(String dumpClass: dumpClasses) {
				SchemaModelDumper schemaDumper = (SchemaModelDumper) Utils.getClassInstance(dumpClass.trim(), DEFAULT_CLASSLOADING_PACKAGES);
				if(schemaDumper!=null) {
					schemaDumper.procProperties(sdd.papp);
					schemaDumper.dumpSchema(sm);
				}
				else {
					log.error("Error initializing dump class: '"+dumpClass+"'");
				}
			}
		}
		else {
			log.debug("no schema dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined");
		}

		//processing classes after dumpers
		if(processingClassesAfterDumpersStr!=null) {
			sdd.processClasses(processingClassesAfterDumpersStr, sm);
		}
		
		}
		finally {
			sdd.end(c==null);
		}
	}
	
	void setupConnection() throws ClassNotFoundException, SQLException, NamingException {
		String connPrefix = papp.getProperty(PROP_CONNPROPPREFIX);
		if(connPrefix==null) {
			connPrefix = CONN_PROPS_PREFIX;
		}
		else {
			log.info("connection properties prefix: '"+connPrefix+"'");
		}
		conn = SQLUtils.ConnectionUtil.initDBConnection(connPrefix, papp);
		DBMSResources.instance().updateMetaData(conn.getMetaData()); //XXX: really needed?
	}
	
	void processClasses(String processingClassesStr, SchemaModel sm) throws ClassNotFoundException, SQLException, NamingException {
		if(conn==null) { setupConnection(); }
		
		String processingClasses[] = processingClassesStr.split(",");
		for(String procClass: processingClasses) {
			Processor sqlproc = (Processor) Utils.getClassInstance(procClass.trim(), DEFAULT_CLASSLOADING_PACKAGES);
			if(sqlproc!=null) {
				sqlproc.setProperties(papp);
				sqlproc.setConnection(conn);
				sqlproc.setSchemaModel(sm);
				//TODO: set fail on error based on properties
				//sqlproc.setFailOnError(true);
				sqlproc.process();
			}
			else {
				log.error("Error initializing processing class: '"+procClass+"'");
			}
		}
	}
	
}
