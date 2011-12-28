package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.log4j.Logger;

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
 * XXX: new dumper: test case dumper: dumps defined records and its parent/child records based on FKs (needs schema and connection)
 * XXXdone: new dumper: alter schema suggestions (PKs, FKs, "create index"s)
 * XXXdone: fixed prop 'propfilebasedir'/'basepropdir': properties file directory
 * XXX: add shutdown option (Derby). see JDBCSchemaGrabber.grabDbSpecificFeaturesClass()
 * XXX: add startup option, before opening connection (SQLite, ...) - readOnlyConnection , ...
 * ~TODO: sqlregen/sqlrun // SQLCreate/SQLRecreate/SQLGenerate/SQLRegenerate: command for sending sql statements to database (re-generate database). order for sending statements based on regex
 * XXXxx: default value for 'sqldump.dumpschemapattern'? user? upper(user)/(oracle)? public (postgresql)? only if contained in MetaData().getSchemas()
 * TODO: more transparent way of selecting index grabbing strategy: 'sqldump.dbspecificfeatures.grabindexes' / 'sqldump.doschemadump.indexes'
 * XXX: FK 'on delete cascade'? UNIQUE constraints 'not null'? other modifiers?
 * ~XXX: create view WITH CHECK OPTION - can only update rows thar are accessible through the view (+ WITH READ ONLY)
 * XXX: add junit tests for all "supported" databases (needs sqlregex first?)
 * XXXxx: error dumping blobs
 * XXX!: add support for blobs (file: <tablename>_<columnname>_<pkid>.blob ? specific prop !) - if table has no PK, no blob dumping
 * XXXxx: add support for cursor in sql (ResultSet as a column type): [x] xml, [x] html, [x] json dumpers
 * XXX: option for queries (or specific queries) to have specific syntax-dumpers
 */
public class SQLDump {
	
	//connection props
	public static final String CONN_PROP_USER = "user";
	public static final String CONN_PROP_PASSWORD = "password";
	
	//connection properties
	public static final String PROP_DRIVERCLASS = "sqldump.driverclass";
	public static final String PROP_URL = "sqldump.dburl";
	public static final String PROP_USER = "sqldump.user";
	public static final String PROP_PASSWD = "sqldump.password";
	public static final String PROP_ASKFORUSERNAME = "sqldump.askforusername";
	public static final String PROP_ASKFORPASSWD = "sqldump.askforpassword";
	public static final String PROP_ASKFORUSERNAME_GUI = "sqldump.askforusernamegui";
	public static final String PROP_ASKFORPASSWD_GUI = "sqldump.askforpasswordgui";
	
	//static/constant properties
	public static final String PROP_PROPFILEBASEDIR = "propfilebasedir"; //"propfiledir" / "propfilebasedir" / "propertiesbasedir" / "basepropdir"
		
	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP = "sqldump.doschemadump";
	static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass";
	static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses";
	static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	
	public static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	public static final String PROP_TO_DB_ID = "sqldump.todbid";
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	static final String PROP_DO_QUERIESDUMP = "sqldump.doqueriesdump";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	public static final String DBMS_SPECIFIC_RESOURCE = "dbms-specific.properties";
	public static final String DEFAULT_CLASSLOADING_PACKAGE = "tbrugz.sqldump"; 
	
	public static final String PARAM_PROPERTIES_FILENAME = "-propfile="; 
	public static final String PARAM_USE_SYSPROPERTIES = "-usesysprop"; 
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	Connection conn;

	Properties papp = new ParametrizedProperties();
	
	boolean doTests = false, 
			doSchemaDump = false, //XXX: default for doSchemaDump should be true?
			doDataDump = false;
	
	void init(String[] args) throws Exception {
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
		log.info("loading properties: "+propFile);
		papp.load(new FileInputStream(propFile));
		
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		papp.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());

		/*try {
			papp.load(new FileInputStream(propFilename));
		}
		catch(FileNotFoundException e) {
			log.warn("file "+propFilename+" not found. loading "+PROPERTIES_FILENAME);			
			papp.load(new FileInputStream(PROPERTIES_FILENAME));
		}*/

		//init control vars
		doSchemaDump = Utils.getPropBool(papp, PROP_DO_SCHEMADUMP, doSchemaDump);
		doTests = Utils.getPropBool(papp, PROP_DO_TESTS, doTests);
		doDataDump = Utils.getPropBool(papp, PROP_DO_DATADUMP, doDataDump); 
	}

	public static Connection initDBConnection(String[] args, Properties papp) throws Exception {
		//init database
		log.debug("initDBConnection...");
		Class.forName(papp.getProperty(PROP_DRIVERCLASS));
		
		Properties p = new Properties();
		p.setProperty(CONN_PROP_USER, papp.getProperty(PROP_USER, ""));
		p.setProperty(CONN_PROP_PASSWORD, papp.getProperty(PROP_PASSWD, ""));
		
		if(Utils.getPropBool(papp, PROP_ASKFORUSERNAME)) {
			p.setProperty(CONN_PROP_USER, Utils.readText("username for '"+papp.getProperty(PROP_URL)+"': "));
		}
		else if(Utils.getPropBool(papp, PROP_ASKFORUSERNAME_GUI)) {
			p.setProperty(CONN_PROP_USER, Utils.readTextGUI("username for '"+papp.getProperty(PROP_URL)+"': "));
		}

		if(Utils.getPropBool(papp, PROP_ASKFORPASSWD)) {
			p.setProperty(CONN_PROP_PASSWORD, Utils.readPassword("password [user="+p.getProperty(CONN_PROP_USER)+"]: "));
		}
		else if(Utils.getPropBool(papp, PROP_ASKFORPASSWD_GUI)) {
			p.setProperty(CONN_PROP_PASSWORD, Utils.readPasswordGUI("password [user="+p.getProperty(CONN_PROP_USER)+"]: "));
		}

		return DriverManager.getConnection(papp.getProperty(PROP_URL), p);
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
		
		//grabbing model
		String grabClassName = sdd.papp.getProperty(PROP_SCHEMAGRAB_GRABCLASS);
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) getClassInstance(grabClassName, DEFAULT_CLASSLOADING_PACKAGE);
			if(schemaGrabber!=null) {
				schemaGrabber.procProperties(sdd.papp);
				if(schemaGrabber.needsConnection() && sdd.conn==null) {
					sdd.conn = initDBConnection(args, sdd.papp);
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

		if(sdd.doTests) {
			if(sdd.conn==null) { sdd.conn = initDBConnection(args, sdd.papp); }
			SQLTests.tests(sdd.conn);
		}
		
		if(Utils.getPropBool(sdd.papp, PROP_DO_QUERIESDUMP)) {
			if(sdd.conn==null) { sdd.conn = initDBConnection(args, sdd.papp); }
			SQLQueries.doQueries(sdd.conn, sdd.papp);
		}
		
		//dumping model
		if(sdd.doSchemaDump) {
			
			String dumpSchemaClasses = sdd.papp.getProperty(PROP_SCHEMADUMP_DUMPCLASSES);
			if(dumpSchemaClasses!=null) {
				String dumpClasses[] = dumpSchemaClasses.split(",");
				for(String dumpClass: dumpClasses) {
					SchemaModelDumper schemaDumper = (SchemaModelDumper) getClassInstance(dumpClass.trim(), DEFAULT_CLASSLOADING_PACKAGE);
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
		
		//dumping data
		//if(sdd.doDataDump && schemaJDBCGrabber!=null && schemaJDBCGrabber.tableNamesForDataDump!=null) {
		if(sdd.doDataDump && schemaGrabber!=null) {
			DataDump dd = new DataDump();
			//dd.dumpData(sdd.conn, schemaJDBCGrabber.tableNamesForDataDump, sdd.papp);
			dd.dumpData(sdd.conn, sm.getTables(), sdd.papp);
		}

		}
		finally {
			log.info("closing connection: "+sdd.conn);
			sdd.end();
		}
	}
	
	static Object getClassInstance(String className, String... defaultPackages) {
		Object o = Utils.getClassInstance(className);
		int countPack = 0;
		while(o==null && defaultPackages!=null && defaultPackages.length > countPack) {
			o = Utils.getClassInstance(defaultPackages[countPack]+"."+className);
			countPack++;
		}
		return o;
	}
	
}
