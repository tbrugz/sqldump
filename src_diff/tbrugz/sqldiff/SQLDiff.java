package tbrugz.sqldiff;

import static tbrugz.sqldump.SQLDump.PARAM_PROPERTIES_FILENAME;
import static tbrugz.sqldump.SQLDump.PARAM_USE_SYSPROPERTIES;
import static tbrugz.sqldump.SQLDump.PROP_PROPFILEBASEDIR;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * TODOne: output diff to file
 * TODOne: CategorizedOut: split by objecttype, schemaname, ...
 * 
 * XXX: output diff by change type
 * XXX: change: 'from'->'old', 'to'->'new' ? or older/newer? original/final? source/target?
 * 
 * XXX: option: [ignore|do not ignore] case; ignore schema name 
 */
public class SQLDiff {
	
	public static final String PROPERTIES_FILENAME = "sqldiff.properties";
	
	public static final String PROP_PREFIX = "sqldiff";
	public static final String PROP_FROM = PROP_PREFIX+".from";
	public static final String PROP_TO = PROP_PREFIX+".to";
	public static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern";

	static Log log = LogFactory.getLog(SQLDiff.class);
	
	Properties prop = new ParametrizedProperties();

	String outfilePattern = null;
	
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
		
		DBObject.dumpCreateOrReplace = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_USECREATEORREPLACE, false);
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);
		
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		if(outfilePattern==null) {
			log.warn("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump diff script");
			return;
		}
	}

	void doIt() throws Exception {
		if(outfilePattern==null) { return; }
		
		SchemaModelGrabber fromSchemaGrabber = null;
		SchemaModelGrabber toSchemaGrabber = null;
		
		//from
		fromSchemaGrabber = initGrabber("from", PROP_FROM, prop);
		
		//to
		toSchemaGrabber = initGrabber("to", PROP_TO, prop);
		
		//grab schemas
		log.info("grabbing 'from' model");
		SchemaModel fromSM = fromSchemaGrabber.grabSchema();
		log.info("grabbing 'to' model");
		SchemaModel toSM = toSchemaGrabber.grabSchema();
		
		//XXX: option to set dialect from properties?
		String dialect = toSM.getSqlDialect();
		log.debug("diff dialect set to: "+dialect);
		DBMSResources.instance().updateDbId(dialect);
		
		CategorizedOut co = new CategorizedOut();
		/*String finalPattern = outfilePattern.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, "\\$\\{1\\}")
				.replaceAll(SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, "\\$\\{2\\}"); //XXX: Matcher.quoteReplacement()? maybe not...*/
		String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern, SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA,SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE);
		log.debug("final pattern: "+finalPattern);
		
		co.setFilePathPattern(finalPattern);

		//do diff
		log.info("dumping diff...");
		SchemaDiff diff = SchemaDiff.diff(fromSM, toSM);
		//co.categorizedOut(diff.getDiff());
		diff.outDiffs(co);
		log.info("...done dumping");
	}
	
	static SchemaModelGrabber initSchemaModelGrabberInstance(String grabClassName) {
		SchemaModelGrabber schemaGrabber = null;
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) SQLDump.getClassInstance(grabClassName, SQLDump.DEFAULT_CLASSLOADING_PACKAGES);
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
		SchemaModelGrabber schemaGrabber = initSchemaModelGrabberInstance(grabClassName);
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
