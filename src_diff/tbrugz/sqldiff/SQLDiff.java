package tbrugz.sqldiff;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;
import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldiff.datadiff.DataDiff;
import tbrugz.sqldiff.model.DBIdentifiableDiff;
import tbrugz.sqldiff.model.Diff;
import tbrugz.sqldiff.model.SchemaDiff;
import tbrugz.sqldiff.model.ColumnDiff;
import tbrugz.sqldiff.model.TableDiff;
import tbrugz.sqldiff.util.RenameDetector;
import tbrugz.sqldiff.validate.DiffValidator;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.processors.DirectoryCleaner;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLIdentifierDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * XXX: output diff by change type
 * 
 * XXX: option: [ignore|do not ignore] case; ignore schema name
 */
public class SQLDiff implements Executor {
	
	public static final String PROPERTIES_FILENAME = "sqldiff.properties";

	//base props
	public static final String PROP_PREFIX = "sqldiff";
	public static final String PROP_SOURCE = PROP_PREFIX+".source";
	public static final String PROP_TARGET = PROP_PREFIX+".target";
	
	//schema input/output props
	static final String PREFIX_INPUT = PROP_PREFIX+".input";
	static final String PREFIX_OUTPUT = PROP_PREFIX+".output";
	public static final String PROP_XMLINFILE = PREFIX_INPUT+".xmlfile";
	public static final String PROP_XMLOUTFILE = PREFIX_OUTPUT+".xmlfile";
	public static final String PROP_JSONINFILE = PREFIX_INPUT+".jsonfile";
	public static final String PROP_JSONOUTFILE = PREFIX_OUTPUT+".jsonfile";
	public static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern"; //XXX: rename to 'sqldiff.output.filepattern'?
	
	//other props
	public static final String PROP_DO_DATADIFF = PROP_PREFIX+".dodatadiff";
	public static final String PROP_FAILONERROR = PROP_PREFIX+".failonerror";
	public static final String PROP_DELETEREGULARFILESDIR = PROP_PREFIX+".deleteregularfilesfromdir";
	public static final String PROP_ADD_COMMENTS = PROP_PREFIX+".addcomments";
	
	//type-dependent props
	public static final String PROP_COLUMNDIFF_TEMPCOLSTRATEGY = PROP_PREFIX+".columndiff.tempcolstrategy";
	public static final String PROP_DBIDDIFF_USEREPLACE = PROP_PREFIX+".dbiddiff.usereplace";

	//rename detection
	public static final String PROP_DO_RENAMEDETECTION = PROP_PREFIX+".dorenamedetection";
	public static final String PROP_RENAMEDETECT_MINSIMILARITY = PROP_PREFIX+".renamedetection.minsimilarity";

	//apply diff props
	static final String PROP_DO_APPLYDIFF = PROP_PREFIX+".doapplydiff";
	static final String PROP_APPLYDIFF_TOSOURCE = PROP_PREFIX+".applydiff.tosource";
	static final String PROP_APPLYDIFF_TOCONN = PROP_PREFIX+".applydiff.toconn";
	static final String PROP_APPLYDIFF_TOID = PROP_PREFIX+".applydiff.toid";
	static final String PROP_APPLYDIFF_VALIDATE = PROP_PREFIX+".applydiff.validate";
	static final String PROP_APPLYDIFF_SCHEMADIFF = PROP_PREFIX+".doapplyschemadiff";
	static final String PROP_APPLYDIFF_DATADIFF = PROP_PREFIX+".doapplydatadiff";
	
	static final String XML_IO_CLASS = "tbrugz.sqldiff.io.XMLDiffIO";
	static final String JSON_IO_CLASS = "tbrugz.sqldiff.io.JSONDiffIO";

	static final Log log = LogFactory.getLog(SQLDiff.class);
	
	final Properties prop = new ParametrizedProperties();

	boolean failonerror = true;
	String outfilePattern = null;
	
	String xmlinfile = null;
	String xmloutfile = null;

	String jsoninfile = null;
	String jsonoutfile = null;
	
	transient int lastDiffCount = 0;
	
	int doIt() throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException {
		
		SchemaModelGrabber fromSchemaGrabber = null;
		SchemaModelGrabber toSchemaGrabber = null;
		SchemaModel fromSM = null;
		SchemaModel toSM = null;
		String sourceId = null;
		String targetId = null;
		
		SchemaDiff diff = null;
		
		long initTime = System.currentTimeMillis();
		
		if(xmlinfile!=null) {
			DiffGrabber dg = (DiffGrabber) Utils.getClassInstance(XML_IO_CLASS);
			diff = (SchemaDiff) dg.grabDiff(new File(xmlinfile));
		}
		else if(jsoninfile!=null) {
			DiffGrabber dg = (DiffGrabber) Utils.getClassInstance(JSON_IO_CLASS);
			diff = (SchemaDiff) dg.grabDiff(new File(jsoninfile));
		}
		else {
		
		//from
		sourceId = prop.getProperty(PROP_SOURCE);
		fromSchemaGrabber = initGrabber("source", sourceId, prop);
		
		//to
		targetId = prop.getProperty(PROP_TARGET);
		toSchemaGrabber = initGrabber("target", targetId, prop);
		
		//grab schemas
		log.info("grabbing 'source' model ["+sourceId+"]");
		fromSM = fromSchemaGrabber.grabSchema();
		log.info("grabbing 'target' model ["+targetId+"]");
		toSM = toSchemaGrabber.grabSchema();
		
		//XXX: option to set dialect from properties?
		String dialect = toSM.getSqlDialect();
		log.debug("diff dialect set to: "+dialect);
		DBMSResources.instance().updateDbId(dialect);

		//do diff
		log.info("diffing...");
		diff = SchemaDiff.diff(fromSM, toSM);
		
		//detect renames
		//XXX: add DiffProcessor?
		boolean doRenameDetection = Utils.getPropBool(prop, PROP_DO_RENAMEDETECTION, false); //XXX: should be true?
		if(doRenameDetection) {
			double minSimilarity = Utils.getPropDouble(prop, PROP_RENAMEDETECT_MINSIMILARITY, 0.5);
			int renames = 0;
			renames += RenameDetector.detectAndDoTableRenames(diff.getTableDiffs(), minSimilarity);
			renames += RenameDetector.detectAndDoColumnRenames(diff.getColumnDiffs(), minSimilarity);
			if(renames>0) {
				SchemaDiff.logInfo(diff);
			}
		}
		
		}
		
		//delete files from dir...
		String dirToDeleteFiles = prop.getProperty(PROP_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			DirectoryCleaner dc = new DirectoryCleaner();
			dc.setDirToDeleteFiles(new File(dirToDeleteFiles));
			dc.process();
		}

		//dump diff
		if(outfilePattern!=null) {
			String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern, 
					new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_SCHEMA, Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME)},
					new String[]{SchemaModelScriptDumper.FILENAME_PATTERN_OBJECTTYPE, Defs.addSquareBraquets(Defs.PATTERN_OBJECTTYPE)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_OBJECTNAME)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE)}
					);
			CategorizedOut co = new CategorizedOut(finalPattern);
			log.debug("final pattern: "+finalPattern);
			
			co.setFilePathPattern(finalPattern);
	
			//co.categorizedOut(diff.getDiff());
			//log.info("dumping diff...");
			diff.outDiffs(co);
		}
		
		if(xmloutfile!=null) {
			try {
				File f = new File(xmloutfile);
				DiffDumper dd = (DiffDumper) Utils.getClassInstance(XML_IO_CLASS);
				dd.dumpDiff(diff, f);
			} catch (JAXBException e) {
				log.warn("error writing xml: "+e);
				log.debug("error writing xml: "+e.getMessage(),e);
			}
		}

		if(jsonoutfile!=null) {
			try {
				File f = new File(jsonoutfile);
				DiffDumper dd = (DiffDumper) Utils.getClassInstance(JSON_IO_CLASS);
				dd.dumpDiff(diff, f);
			} catch (JAXBException e) {
				log.warn("error writing json: "+e);
				log.debug("error writing json: "+e.getMessage(),e);
			}
		}

		boolean doDataDiff = Utils.getPropBool(prop, PROP_DO_DATADIFF, false);
		boolean doApplyDiff = Utils.getPropBool(prop, PROP_DO_APPLYDIFF, false);
		boolean doApplySchemaDiff = doApplyDiff && Utils.getPropBool(prop, PROP_APPLYDIFF_SCHEMADIFF, false);
		boolean doApplyDataDiff = doApplyDiff && Utils.getPropBool(prop, PROP_APPLYDIFF_DATADIFF, false);
		boolean doApplyValidate = Utils.getPropBool(prop, PROP_APPLYDIFF_VALIDATE, true);
		
		boolean applyToSource = Utils.getPropBool(prop, PROP_APPLYDIFF_TOSOURCE, false);
		String applyToId = prop.getProperty(PROP_APPLYDIFF_TOID);
		String applyToConnPrefix = prop.getProperty(PROP_APPLYDIFF_TOCONN);
		
		if(doApplyDiff && !doApplySchemaDiff && !doApplyDataDiff) {
			String message = "apply diff prop defined, but no schemadiff ('"+PROP_APPLYDIFF_SCHEMADIFF+"') nor datadiff ('"+PROP_APPLYDIFF_DATADIFF+"') properties defined";
			log.warn(message);
			if(failonerror) { throw new ProcessingException(message); }
		}
		
		//apply schema diff to database?
		if(doApplySchemaDiff) {
			Connection applyToConn = null;
			SchemaModel applyToModel = null;
			
			if(applyToSource) {
				if(fromSchemaGrabber!=null) {
					applyToConn = fromSchemaGrabber.getConnection();
					applyToModel = fromSM;
				}
				if(applyToConn==null) {
					//if source was not grabbed from JDBC/connection
					String connPrefix = "sqldiff."+prop.getProperty(PROP_SOURCE);
					log.info("initting 'source' connection to apply diff [prefix = '"+connPrefix+"']");
					applyToConn = ConnectionUtil.initDBConnection(connPrefix, prop);
				}
			}
			else if(applyToId!=null) {
				if(applyToId.equals(sourceId)) {
					applyToModel = fromSM;
				}
				else if(applyToId.equals(targetId)) {
					applyToModel = toSM;
				}
				else {
					SchemaModelGrabber applyToGrabber = initGrabber("apply-to", applyToId, prop);
					log.info("grabbing 'apply-to' model ["+applyToId+"]");
					applyToModel = applyToGrabber.grabSchema();
					applyToConn = applyToGrabber.getConnection();
				}
				if(applyToConn==null) {
					String connPrefix = "sqldiff."+applyToId;
					log.info("initting 'apply-to' connection to apply diff [prefix = '"+connPrefix+"']");
					applyToConn = ConnectionUtil.initDBConnection(connPrefix, prop);
				}
			}
			else if(applyToConnPrefix!=null) {
				log.info("initting connection to apply diff [prefix = '"+applyToConnPrefix+"']");
				applyToConn = ConnectionUtil.initDBConnection(applyToConnPrefix, prop);
			}
			else {
				String message = "applydiff (ditt-to-db) target (prop '"+PROP_APPLYDIFF_TOSOURCE+"' or '"+PROP_APPLYDIFF_TOCONN+"') not defined";
				log.warn(message);
				if(failonerror) { throw new ProcessingException(message); }
			}
				
			if(applyToConn==null) {
				log.warn("connection is null!");
				//XXX: throw exception?
			}
			else {
				if(applyToModel!=null) {
					if(doApplyValidate) {
						DiffValidator dv = new DiffValidator(applyToModel);
						dv.validateDiff(diff);
					}
				}
				else {
					log.info("no 'apply-to' model defined, diff may not be validated");
				}
				DBMSResources.instance().updateMetaData(applyToConn.getMetaData());
				applyDiffToDB(diff, applyToConn);
			}
		}

		//data diff!
		if(doDataDiff) {
			DataDiff dd = new DataDiff();
			dd.setFailOnError(failonerror);
			//XXX: some refactoring would be nice...
			if(applyToSource) {
				String sourceGrabberId = prop.getProperty(PROP_SOURCE);
				prop.setProperty("sqldiff.datadiff.sdd2db.connpropprefix", "sqldiff."+sourceGrabberId);
			}
			dd.setProperties(prop);
			dd.setApplyDataDiff(doApplyDataDiff);
			dd.setSourceSchemaModel(fromSM);
			if(fromSchemaGrabber!=null) {
				dd.setSourceConnection(fromSchemaGrabber.getConnection());
			}
			dd.setTargetSchemaModel(toSM);
			if(toSchemaGrabber!=null) {
				dd.setTargetConnection(toSchemaGrabber.getConnection());
			}
			dd.process();
		}
		
		//XXX close connections if open?
		
		log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		
		return diff.getDiffList().size();
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
	
	static SchemaModelGrabber initGrabber(String grabberLabel, String grabberId, Properties prop) throws ClassNotFoundException, SQLException, NamingException {
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
			Connection conn = ConnectionUtil.initDBConnection("sqldiff."+grabberId, prop);
			schemaGrabber.setConnection(conn);
		}
		schemaGrabber.setProperties(prop);
		return schemaGrabber;
	}
	
	void procProterties() {
		failonerror = Utils.getPropBool(prop, PROP_FAILONERROR, failonerror);
		DBObject.dumpCreateOrReplace = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_USECREATEORREPLACE, false);
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		xmlinfile = prop.getProperty(PROP_XMLINFILE);
		xmloutfile = prop.getProperty(PROP_XMLOUTFILE);
		jsoninfile = prop.getProperty(PROP_JSONINFILE);
		jsonoutfile = prop.getProperty(PROP_JSONOUTFILE);
		String colDiffTempStrategy = prop.getProperty(PROP_COLUMNDIFF_TEMPCOLSTRATEGY);
		if(colDiffTempStrategy!=null) {
			try {
				ColumnDiff.useTempColumnStrategy = ColumnDiff.TempColumnAlterStrategy.valueOf(colDiffTempStrategy.toUpperCase());
			}
			catch(IllegalArgumentException e) {
				String message = "illegal value '"+colDiffTempStrategy+"' to prop '"+PROP_COLUMNDIFF_TEMPCOLSTRATEGY+"' [default is '"+ColumnDiff.useTempColumnStrategy+"']";
				log.warn(message);
				if(failonerror) {
					throw new ProcessingException(message, e);
				}
			}
		}
		boolean addComments = Utils.getPropBool(prop, PROP_ADD_COMMENTS, true);
		ColumnDiff.addComments = addComments;
		DBIdentifiableDiff.addComments = addComments;
		TableDiff.mayReplaceDbId = Utils.getPropBool(prop, PROP_DBIDDIFF_USEREPLACE, TableDiff.mayReplaceDbId);
	}
	
	void applyDiffToDB(SchemaDiff diff, Connection conn) {
		List<Diff> diffs = diff.getChildren();

		int diffCount = 0;
		int execCount = 0;
		int errorCount = 0;
		int updateCount = 0;
		SQLException lastEx = null; 
		for(Diff d: diffs) {
			try {
				//XXX: option to send all SQLs from one diff in only one statement? no problem for h2...  
				diffCount++;
				List<String> sqls = d.getDiffList();
				for(int i=0;i<sqls.size();i++) {
					String sql = sqls.get(i);
					log.info("executing diff #"+diffCount
						+(sqls.size()>1?" ["+(i+1)+"/"+sqls.size()+"]: ":": ")
						+sql);
					execCount++;
					updateCount += conn.createStatement().executeUpdate(sql);
				}
			} catch (SQLException e) {
				errorCount++;
				lastEx = e;
				log.warn("error executing diff: "+e);
				if(failonerror) { break; }
			}
		}
		if(execCount>0) {
			log.info(execCount+" statements executed"
				+(errorCount>0?" [#errors = "+errorCount+"]":"")
				+" [#update = "+updateCount+"]");
		}
		else {
			log.info("no diff statements executed");
		}
		
		if(failonerror && errorCount>0) {
			throw new ProcessingException(errorCount+" sqlExceptions occured", lastEx);
		}
	}

	@Override
	public void doMain(String[] args, Properties properties) throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException {
		if(properties!=null) {
			prop.putAll(properties);
		}
		CLIProcessor.init("sqldiff", args, PROPERTIES_FILENAME, prop);
		procProterties();
		DBMSResources.instance().setup(prop);

		if(outfilePattern==null && xmloutfile==null) {
			String message = "outfilepattern [prop '"+PROP_OUTFILEPATTERN+"'] nor xmloutfile [prop '"+PROP_XMLOUTFILE+"'] nor jsonoutfile [prop '"+PROP_JSONOUTFILE+"'] defined. can't dump diff script";
			log.error(message);
			if(failonerror) { throw new ProcessingException(message); }
			return;
		}
		
		lastDiffCount = doIt();
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException {
		SQLDiff sqldiff = new SQLDiff();
		sqldiff.doMain(args, sqldiff.prop);
	}
	
	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}
	
	public int getLastDiffCount() {
		return lastDiffCount;
	}
}
