package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import org.apache.log4j.Logger;

import tbrugz.sqldump.graph.Schema2GraphML;

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
 * XXX: COMMENT/REMARKS for tables
 * TODOne: bitbucket project's wiki
 * TODOne: main(): args: point to different .properties init files. 
 * XXXdone: Use ${xxx} params inside Properties
 * XXXdone: data dump: limit tables to dump 
 * XXXxx: define output patterns for data dump
 * TODO: include demo schema and data
 * XXX: option to delete initial output dir contents (except special hidden files (unix dotfiles) eg: .svn, .git, .hg)?
 * ---
 * XXXxxx: compare 2 schema models? generate "alter table" database script... see SQLDiff
 * XXX(later): generate schema model from graphML file (XMLUnit?). may be used for model comparison 
 * XXXdone: serialize model (for later comparison)
 * XXXdone: XML schema model grabber/dumper - http://en.wikipedia.org/wiki/XML_data_binding, http://stackoverflow.com/questions/35785/xml-serialization-in-java, http://www.castor.org/xml-framework.html
 *   - x jaxb, xtream, xmlbeans, x castor, jibx
 */
public class SQLDump {
	
	//connection properties
	static final String PROP_DRIVERCLASS = "sqldump.driverclass";
	static final String PROP_URL = "sqldump.dburl";
	static final String PROP_USER = "sqldump.user";
	static final String PROP_PASSWD = "sqldump.password";
	static final String PROP_ASKFORPASSWD = "sqldump.askforpassword";
		
	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP = "sqldump.doschemadump";
	static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass";
	static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses";
	static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	
	public static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	public static final String PROP_TO_DB_ID = "sqldump.todbid";
	static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	static final String PROP_FROM_DB_ID_AUTODETECT = "sqldump.fromdbid.autodetect";
	static final String PROP_DO_QUERIESDUMP = "sqldump.doqueriesdump";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	public static final String COLUMN_TYPE_MAPPING_RESOURCE = "column-type-mapping.properties";
	public static final String DEFAULT_CLASSLOADING_PACKAGE = "tbrugz.sqldump"; 
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	Connection conn;

	Properties papp = new ParametrizedProperties();
	
	boolean doTests = false, doSchemaDump = false, doDataDump = false;
	
	static final String PARAM_PROPERTIES_FILENAME = "-propfile="; 
	
	void init(String[] args) throws Exception {
		log.info("init...");
		//parse args
		String propFilename = PROPERTIES_FILENAME;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
			}
			else {
				log.warn("unrecognized param '"+arg+"'. ignoring...");
			}
		}

		//init properties
		log.info("loading properties: "+propFilename);
		papp.load(new FileInputStream(propFilename));
		/*try {
			papp.load(new FileInputStream(propFilename));
		}
		catch(FileNotFoundException e) {
			log.warn("file "+propFilename+" not found. loading "+PROPERTIES_FILENAME);			
			papp.load(new FileInputStream(PROPERTIES_FILENAME));
		}*/

		//init control vars
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doDataDump = papp.getProperty(PROP_DO_DATADUMP, "").equals("true"); 
	}

	void initDBDriver(String[] args) throws Exception {
		//init database
		Class.forName(papp.getProperty(PROP_DRIVERCLASS));
		
		Properties p = new Properties();
		p.setProperty("user", papp.getProperty(PROP_USER, ""));
		p.setProperty("password", papp.getProperty(PROP_PASSWD, ""));
		
		if(Utils.getPropBool(papp, PROP_ASKFORPASSWD)) {
			p.setProperty("password", Utils.readPassword("password [user="+p.getProperty("user")+"]: "));
		}

		conn = DriverManager.getConnection(papp.getProperty(PROP_URL), p);
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

		sdd.init(args);
		
		SchemaModel sm = null;
		SchemaModelGrabber schemaGrabber = null;
		
		//grabbing model
		String grabClassName = sdd.papp.getProperty(PROP_SCHEMAGRAB_GRABCLASS);
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) getClassInstance(grabClassName, DEFAULT_CLASSLOADING_PACKAGE);
			if(schemaGrabber!=null) {
				schemaGrabber.procProperties(sdd.papp);
				if(schemaGrabber.needsConnection() && sdd.conn==null) {
					sdd.initDBDriver(args);
				}
				schemaGrabber.setConnection(sdd.conn);
				sm = schemaGrabber.grabSchema();
			}
			else {
				log.warn("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.info("no schema grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		
		//SchemaModelGrabber schemaJDBCGrabber = new JDBCSchemaGrabber();
		//schemaJDBCGrabber.procProperties(sdd.papp);
		//schemaJDBCGrabber.setConnection(sdd.conn);
		//SchemaModel sm = schemaJDBCGrabber.grabSchema();

		//serializer input
		//SchemaModelGrabber schemaSerialGrabber = new SchemaSerializer();
		//schemaSerialGrabber.procProperties(sdd.papp);
		//SchemaModel sm = schemaSerialGrabber.grabSchema();

		//xml serializer input
		//SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		//schemaSerialGrabber.procProperties(sdd.papp);
		//SchemaModel sm = schemaSerialGrabber.grabSchema();
		
		String dirToDeleteFiles = sdd.papp.getProperty(PROP_DO_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			Utils.deleteDirRegularContents(dirToDeleteFiles);
		}

		if(sdd.doTests) {
			SQLTests.tests(sdd.conn);
		}
		
		if(Utils.getPropBool(sdd.papp, PROP_DO_QUERIESDUMP)) {
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
				log.info("no schema dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined");
			}
			
			/*
			//script dump
			SchemaModelDumper schemaDumper = new SchemaModelScriptDumper();
			schemaDumper.procProperties(sdd.papp);
			schemaDumper.dumpSchema(sm);
			
			//TODO prop doSerializeDump? doGraphMLDump?

			//serialize
			SchemaModelDumper schemaSerialDumper = new SchemaSerializer();
			schemaSerialDumper.procProperties(sdd.papp);
			schemaSerialDumper.dumpSchema(sm);
			
			//xml serializer (JAXB)
			SchemaModelDumper schemaXMLDumper = new JAXBSchemaXMLSerializer();
			schemaXMLDumper.procProperties(sdd.papp);
			schemaXMLDumper.dumpSchema(sm);

			//xml serializer (Castor)
			SchemaModelDumper schemaCastorXMLDumper = new CastorSchemaXMLSerializer();
			schemaCastorXMLDumper.procProperties(sdd.papp);
			schemaCastorXMLDumper.dumpSchema(sm);
			
			//graphml dump
			SchemaModelDumper s2gml = new Schema2GraphML();
			s2gml.procProperties(sdd.papp);
			s2gml.dumpSchema(sm);
			
			*/
		}
		
		//dumping data
		//if(sdd.doDataDump && schemaJDBCGrabber!=null && schemaJDBCGrabber.tableNamesForDataDump!=null) {
		if(sdd.doDataDump && schemaGrabber!=null) {
			DataDump dd = new DataDump();
			//dd.dumpData(sdd.conn, schemaJDBCGrabber.tableNamesForDataDump, sdd.papp);
			dd.dumpData(sdd.conn, sm.getTables(), sdd.papp);
		}
		
		sdd.end();
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
