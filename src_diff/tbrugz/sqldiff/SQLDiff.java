package tbrugz.sqldiff;

import static tbrugz.sqldump.SQLDump.PARAM_PROPERTIES_FILENAME;
import static tbrugz.sqldump.SQLDump.PARAM_USE_SYSPROPERTIES;
import static tbrugz.sqldump.SQLDump.PROP_PROPFILEBASEDIR;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.ParametrizedProperties;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.SchemaModel;
import tbrugz.sqldump.SchemaModelGrabber;

/*
 * TODO: output diff by object type, change type
 */
public class SQLDiff {
	
	public static final String PROPERTIES_FILENAME = "sqldiff.properties";
	
	public static final String PROP_FROM = "sqldiff.from";
	public static final String PROP_TO = "sqldiff.to";

	static Logger log = Logger.getLogger(SQLDiff.class);
	
	Properties prop = new ParametrizedProperties();
	
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
		prop.load(new FileInputStream(propFile));
		
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		prop.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
	}

	void doIt() throws Exception {
		SchemaModelGrabber fromSchemaGrabber = null;
		SchemaModelGrabber toSchemaGrabber = null;
		
		//from
		fromSchemaGrabber = initGrabber("from", PROP_FROM, prop);
		/*{
		String fromId = prop.getProperty(PROP_FROM);
		log.info("target 'from' ["+fromId+"] init");
		String fromGrabClassName = prop.getProperty("sqldiff."+fromId+".grabclass");
		fromSchemaGrabber = initModelGrabber(fromGrabClassName);
		fromSchemaGrabber.setPropertiesPrefix("sqldiff."+fromId);
		if(fromSchemaGrabber.needsConnection()) {
			Connection conn = SQLUtils.ConnectionUtil.initDBConnection("sqldiff."+fromId, prop);
			fromSchemaGrabber.setConnection(conn);
		}
		fromSchemaGrabber.procProperties(prop);
		}*/
		
		//to
		toSchemaGrabber = initGrabber("to", PROP_TO, prop);
		/*{
		String toId = prop.getProperty(PROP_TO);
		log.info("target 'to' ["+toId+"] init");
		String toGrabClassName = prop.getProperty("sqldiff."+toId+".grabclass");
		toSchemaGrabber = initModelGrabber(toGrabClassName);
		toSchemaGrabber.setPropertiesPrefix("sqldiff."+toId);
		if(toSchemaGrabber.needsConnection()) {
			Connection conn = SQLUtils.ConnectionUtil.initDBConnection("sqldiff."+toId, prop);
			toSchemaGrabber.setConnection(conn);
		}
		toSchemaGrabber.procProperties(prop);
		}*/
		
		//grab schemas
		log.info("grabbing 'from' model");
		SchemaModel fromSM = fromSchemaGrabber.grabSchema();
		log.info("grabbing 'to' model");
		SchemaModel toSM = toSchemaGrabber.grabSchema();
		
		//do diff
		log.info("dumping diff");
		SchemaDiff diff = SchemaDiff.diff(fromSM, toSM);
		System.out.println("=========+=========+=========+=========+=========+=========+=========+=========");
		System.out.println("diff:\n"+diff.getDiff());
		System.out.println("=========+=========+=========+=========+=========+=========+=========+=========");
		
		//List<DBObjectType> objtypeList = Arrays.asList(DBObjectType.TABLE, DBObjectType.COLUMN);
		//System.out.println("diff [types:"+objtypeList+"]\n"+diff.getDiffByDBObjectTypes(objtypeList));
	}
	
	static SchemaModelGrabber initModelGrabberInstance(String grabClassName) {
		SchemaModelGrabber schemaGrabber = null;
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) SQLDump.getClassInstance(grabClassName, SQLDump.DEFAULT_CLASSLOADING_PACKAGE);
			if(schemaGrabber==null) {
				log.warn("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.warn("null grab class name!");
		}
		return schemaGrabber;
	}
	
	static SchemaModelGrabber initGrabber(String targetName, String propKey, Properties prop) throws Exception {
		String grabberId = prop.getProperty(propKey);
		log.info("target '"+targetName+"' ["+grabberId+"] init");
		String grabClassName = prop.getProperty("sqldiff."+grabberId+".grabclass");
		SchemaModelGrabber schemaGrabber = initModelGrabberInstance(grabClassName);
		schemaGrabber.setPropertiesPrefix("sqldiff."+grabberId);
		if(schemaGrabber.needsConnection()) {
			Connection conn = SQLUtils.ConnectionUtil.initDBConnection("sqldiff."+grabberId, prop);
			schemaGrabber.setConnection(conn);
		}
		schemaGrabber.procProperties(prop);
		return schemaGrabber;
	}
	
	public static void main(String[] args) throws Exception {
		SQLDiff sqldiff = new SQLDiff();
		sqldiff.init(args);
		sqldiff.doIt();
	}
}
