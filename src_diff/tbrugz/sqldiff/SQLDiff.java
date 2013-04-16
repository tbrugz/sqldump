package tbrugz.sqldiff;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.datadiff.DataDiff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.util.CLIProcessor;
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
	public static final String PROP_XMLOUTFILE = PROP_PREFIX+".xmloutfile";
	public static final String PROP_DO_DATADIFF = PROP_PREFIX+".dodatadiff";

	static final Log log = LogFactory.getLog(SQLDiff.class);
	
	Properties prop = new ParametrizedProperties();

	boolean failonerror = true;
	String outfilePattern = null;
	String xmloutfile = null;
	
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
				new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME)},
				new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, Defs.addSquareBraquets(Defs.PATTERN_OBJECTTYPE)},
				new String[]{Defs.addSquareBraquets(Defs.PATTERN_OBJECTNAME)},
				new String[]{Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE)}
				);
		CategorizedOut co = new CategorizedOut(finalPattern);
		log.debug("final pattern: "+finalPattern);
		
		co.setFilePathPattern(finalPattern);

		//do diff
		log.info("dumping diff...");
		SchemaDiff diff = SchemaDiff.diff(fromSM, toSM);
		//co.categorizedOut(diff.getDiff());
		diff.outDiffs(co);
		
		if(xmloutfile!=null) {
			try {
				File f = new File(xmloutfile);
				diff.outDiffsXML(f);
			} catch (JAXBException e) {
				log.warn("error writing xml: "+e);
				log.debug("error writing xml: "+e.getMessage(),e);
			}
		}
		
		//data diff!
		boolean doDataDiff = Utils.getPropBool(prop, PROP_DO_DATADIFF, false);
		if(doDataDiff) {
			DataDiff dd = new DataDiff();
			dd.setFailOnError(failonerror);
			dd.setProperties(prop);
			dd.setSourceSchemaModel(fromSM);
			dd.setSourceConnection(fromSchemaGrabber.getConnection());
			dd.setTargetSchemaModel(toSM);
			dd.setTargetConnection(toSchemaGrabber.getConnection());
			dd.process();
		}
		
		//XXX close connections if open?
		
		log.info("...done dumping");
	}
	
	static SchemaModelGrabber initSchemaModelGrabberInstance(String grabClassName) {
		SchemaModelGrabber schemaGrabber = null;
		if(grabClassName!=null) {
			schemaGrabber = (SchemaModelGrabber) Utils.getClassInstance(grabClassName, Defs.DEFAULT_CLASSLOADING_PACKAGES);
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
		
		CLIProcessor.init("sqldiff", args, PROPERTIES_FILENAME, sqldiff.prop);
		DBObject.dumpCreateOrReplace = Utils.getPropBool(sqldiff.prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_USECREATEORREPLACE, false);
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(sqldiff.prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);
		sqldiff.outfilePattern = sqldiff.prop.getProperty(PROP_OUTFILEPATTERN);
		if(sqldiff.outfilePattern==null) {
			log.error("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump diff script");
			if(sqldiff.failonerror) { throw new ProcessingException("outfilepattern not defined [prop '"+PROP_OUTFILEPATTERN+"']. can't dump diff script"); }
			return;
		}
		sqldiff.xmloutfile = sqldiff.prop.getProperty(PROP_XMLOUTFILE);
		
		sqldiff.doIt();
	}
}
