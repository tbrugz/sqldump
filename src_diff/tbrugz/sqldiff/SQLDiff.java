package tbrugz.sqldiff;

import static tbrugz.sqldump.SQLDump.PARAM_PROPERTIES_FILENAME;
import static tbrugz.sqldump.SQLDump.PARAM_USE_SYSPROPERTIES;
import static tbrugz.sqldump.SQLDump.PROP_PROPFILEBASEDIR;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.datadiff.DataDiff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: output diff by change type
 * 
 * XXX: option: [ignore|do not ignore] case; ignore schema name
 */
public class SQLDiff {
	
	public static final String PROPERTIES_FILENAME = "sqldiff.properties";
	
	public static final String PROP_PREFIX = "sqldiff";
	public static final String PROP_SOURCE = PROP_PREFIX+".source";
	public static final String PROP_TARGET = PROP_PREFIX+".target";
	public static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";
	public static final String PROP_DO_DATADIFF = PROP_PREFIX+".dodatadiff";

	static Log log = LogFactory.getLog(SQLDiff.class);
	
	Properties prop = new ParametrizedProperties();

	String outfilePattern = null;
	
	void init(String[] args) throws FileNotFoundException, IOException {
		log.info("init...");
		//parse args
		boolean useSysPropSetted = false;		
		String propFilename = PROPERTIES_FILENAME;
		for(String arg: args) {
			if(arg.indexOf(PARAM_PROPERTIES_FILENAME)==0) {
				propFilename = arg.substring(PARAM_PROPERTIES_FILENAME.length());
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
		File propFile = new File(propFilename);
		
		//init properties
		log.info("loading properties: "+propFile);
		prop.load(new FileInputStream(propFile));
		
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		prop.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
		
		DBObject.dumpCreateOrReplace = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_USECREATEORREPLACE, false);
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);
		
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		if(outfilePattern==null) {
			log.warn("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump diff script");
			return;
		}
	}

	void doIt() throws ClassNotFoundException, SQLException, NamingException, IOException {
		if(outfilePattern==null) { return; }
		
		SchemaModelGrabber fromSchemaGrabber = null;
		SchemaModelGrabber toSchemaGrabber = null;
		
		//from
		fromSchemaGrabber = initGrabber("source", PROP_SOURCE, prop);
		
		//to
		toSchemaGrabber = initGrabber("target", PROP_TARGET, prop);
		
		//grab schemas
		log.info("grabbing 'source' model");
		SchemaModel fromSM = fromSchemaGrabber.grabSchema();
		log.info("grabbing 'target' model");
		SchemaModel toSM = toSchemaGrabber.grabSchema();
		
		//XXX: option to set dialect from properties?
		String dialect = toSM.getSqlDialect();
		log.debug("diff dialect set to: "+dialect);
		DBMSResources.instance().updateDbId(dialect);
		
		/*String finalPattern = outfilePattern.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, "\\$\\{1\\}")
				.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, "\\$\\{2\\}"); //XXX: Matcher.quoteReplacement()? maybe not...*/
		String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern, 
				new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, Defs.PATTERN_SCHEMANAME},
				new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, Defs.PATTERN_OBJECTTYPE},
				new String[]{Defs.PATTERN_OBJECTNAME},
				new String[]{Defs.PATTERN_CHANGETYPE}
				);
		CategorizedOut co = new CategorizedOut(finalPattern);
		log.debug("final pattern: "+finalPattern);
		
		co.setFilePathPattern(finalPattern);

		//do diff
		log.info("dumping diff...");
		SchemaDiff diff = SchemaDiff.diff(fromSM, toSM);
		//co.categorizedOut(diff.getDiff());
		diff.outDiffs(co);
		
		//data diff!
		boolean doDataDiff = Utils.getPropBool(prop, PROP_DO_DATADIFF, false);
		if(doDataDiff) {
			DataDiff dd = new DataDiff();
			dd.setProperties(prop);
			dd.setSourceSchemaModel(fromSM);
			dd.setSourceConnection(fromSchemaGrabber.getConnection());
			dd.setTargetSchemaModel(toSM);
			dd.setTargetConnection(toSchemaGrabber.getConnection());
			dd.process();
		}
		
		log.info("...done dumping");
	}
	
	static SchemaModelGrabber initSchemaModelGrabberInstance(String grabClassName) {
		SchemaModelGrabber schemaGrabber = null;
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) Utils.getClassInstance(grabClassName, SQLDump.DEFAULT_CLASSLOADING_PACKAGES);
			if(schemaGrabber==null) {
				log.warn("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.warn("null grab class name!");
		}
		return schemaGrabber;
	}
	
	static SchemaModelGrabber initGrabber(String grabberLabel, String propKey, Properties prop) throws ClassNotFoundException, SQLException, NamingException {
		String grabberId = prop.getProperty(propKey);
		if(grabberId==null || "".equals(grabberId)) {
			throw new ProcessingException("'"+grabberLabel+"' grabber id not defined");
		}
		
		log.info(grabberLabel+" model ["+grabberId+"] init");
		String grabClassName = prop.getProperty("sqldiff."+grabberId+".grabclass");
		if(grabClassName==null) {
			throw new ProcessingException("'"+grabberLabel+"' grabber class (id="+grabberId+") not defined");
		}
		
		SchemaModelGrabber schemaGrabber = initSchemaModelGrabberInstance(grabClassName);
		schemaGrabber.setPropertiesPrefix("sqldiff."+grabberId);
		if(schemaGrabber.needsConnection()) {
			Connection conn = SQLUtils.ConnectionUtil.initDBConnection("sqldiff."+grabberId, prop);
			schemaGrabber.setConnection(conn);
		}
		schemaGrabber.procProperties(prop);
		return schemaGrabber;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDiff sqldiff = new SQLDiff();
		sqldiff.init(args);
		sqldiff.doIt();
	}
}
