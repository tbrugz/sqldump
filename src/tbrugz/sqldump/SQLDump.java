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
 * XXX: DEFAULT & COMMENT/REMARKS for columns (& tables?)
 * TODOne: bitbucket project's wiki
 * TODOne: main(): args: point to different .properties init files. 
 * XXXdone: Use ${xxx} params inside Properties
 * XXXdone: data dump: limit tables to dump 
 * XXX?: define output patterns for data dump
 * TODO: include demo schema and data
 * XXX: option to delete initial output dir?
 * ---
 * XXX: compare 2 schema models? generate "alter table" database script...
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

	//sqldump.properties
	static final String PROP_DO_SCHEMADUMP = "sqldump.doschemadump";
	public static final String PROP_FROM_DB_ID = "sqldump.fromdbid";
	public static final String PROP_TO_DB_ID = "sqldump.todbid";
	static final String PROP_DUMP_WITH_SCHEMA_NAME = "sqldump.dumpwithschemaname";
	static final String PROP_DO_TESTS = "sqldump.dotests";
	static final String PROP_DO_DATADUMP = "sqldump.dodatadump";
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	public static final String COLUMN_TYPE_MAPPING_RESOURCE = "column-type-mapping.properties";
	
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

		//init database
		Class.forName(papp.getProperty(PROP_DRIVERCLASS));

		Properties p = new Properties();
		p.setProperty("user", papp.getProperty(PROP_USER, ""));
		p.setProperty("password", papp.getProperty(PROP_PASSWD, ""));

		conn = DriverManager.getConnection(papp.getProperty(PROP_URL), p);
		
		//init control vars
		doSchemaDump = papp.getProperty(PROP_DO_SCHEMADUMP, "").equals("true");
		doTests = papp.getProperty(PROP_DO_TESTS, "").equals("true");
		doDataDump = papp.getProperty(PROP_DO_DATADUMP, "").equals("true"); 
	}

	void end() throws Exception {
		log.info("...done");
		conn.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SQLDump sdd = new SQLDump();

		sdd.init(args);
		
		if(sdd.doTests) {
			SQLTests.tests(sdd.conn);
		}
		
		//grabbing model
		JDBCSchemaGrabber schemaJDBCGrabber = new JDBCSchemaGrabber();
		schemaJDBCGrabber.procProperties(sdd.papp);
		schemaJDBCGrabber.setConnection(sdd.conn);
		SchemaModel sm = schemaJDBCGrabber.grabSchema();

		//serializer input
		//SchemaModelGrabber schemaSerialGrabber = new SchemaSerializer();
		//schemaSerialGrabber.procProperties(sdd.papp);
		//SchemaModel sm = schemaSerialGrabber.grabSchema();

		//xml serializer input
		//SchemaModelGrabber schemaSerialGrabber = new JAXBSchemaXMLSerializer();
		//schemaSerialGrabber.procProperties(sdd.papp);
		//SchemaModel sm = schemaSerialGrabber.grabSchema();
		
		if(sdd.doSchemaDump) {
			
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
		}
		if(sdd.doDataDump && schemaJDBCGrabber!=null && schemaJDBCGrabber.tableNamesForDataDump!=null) {
			DataDump dd = new DataDump();
			dd.dumpData(sdd.conn, schemaJDBCGrabber.tableNamesForDataDump, sdd.papp);
		}
		
		sdd.end();
	}
	
}
