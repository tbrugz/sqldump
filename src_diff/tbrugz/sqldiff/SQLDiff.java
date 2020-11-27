package tbrugz.sqldiff;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
import tbrugz.sqldiff.validate.DiffValidator;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SchemaModelScriptDumper;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.def.ProcessComponent;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.processors.DirectoryCleaner;
import tbrugz.sqldump.processors.SQLDialectTransformer;
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
	public static final String PRODUCT_NAME = "sqldiff";

	public static final String ID_SOURCE = "source";
	public static final String ID_TARGET = "target";
	
	//base props
	public static final String PROP_PREFIX = "sqldiff";
	public static final String PROP_SOURCE = PROP_PREFIX+"."+ID_SOURCE;
	public static final String PROP_TARGET = PROP_PREFIX+"."+ID_TARGET;
	public static final String PROP_TYPES_TO_DIFF = PROP_PREFIX+".typestodiff";
	
	//schema input/output props
	static final String PREFIX_INPUT = PROP_PREFIX+".input";
	static final String PREFIX_OUTPUT = PROP_PREFIX+".output";
	public static final String PROP_XMLINFILE = PREFIX_INPUT+".xmlfile";
	public static final String PROP_XMLOUTFILE = PREFIX_OUTPUT+".xmlfile";
	public static final String PROP_JSONINFILE = PREFIX_INPUT+".jsonfile";
	public static final String PROP_JSONOUTFILE = PREFIX_OUTPUT+".jsonfile";
	public static final String PROP_OUTFILEPATTERN = PROP_PREFIX+".outfilepattern"; //XXX: rename to 'sqldiff.output.filepattern'?
	//patch-output
	public static final String PREFIX_PATCH = PREFIX_OUTPUT+".patch";
	public static final String PROP_PATCHFILEPATTERN = PREFIX_PATCH+".file";
	
	//other props
	public static final String PROP_DO_DATADIFF = PROP_PREFIX+".dodatadiff";
	public static final String PROP_FAILONERROR = PROP_PREFIX+".failonerror";
	public static final String PROP_DELETEREGULARFILESDIR = PROP_PREFIX+".deleteregularfilesfromdir";
	public static final String PROP_ADD_COMMENTS = PROP_PREFIX+".addcomments";
	
	//schemadiff
	public static final String PROP_SCHEMADIFF_TARGET_DIALECT_TO_SOURCE = PROP_PREFIX+".schemadiff.transform-target-dialect-to-source";
	public static final String PROP_SCHEMADIFF_SOURCE_PROCESSORS = PROP_PREFIX+".schemadiff.source.processors";
	public static final String PROP_SCHEMADIFF_TARGET_PROCESSORS = PROP_PREFIX+".schemadiff.target.processors";
	
	//type-dependent props
	public static final String PROP_COLUMNDIFF_TEMPCOLSTRATEGY = PROP_PREFIX+".columndiff.tempcolstrategy";
	public static final String PROP_DBIDDIFF_USEREPLACE = PROP_PREFIX+".dbiddiff.usereplace";

	//rename detection
	public static final String PROP_DO_RENAMEDETECTION = PROP_PREFIX+".dorenamedetection";
	public static final String PROP_RENAMEDETECT_MINSIMILARITY = PROP_PREFIX+".renamedetection.minsimilarity";
	public static final String PROP_RENAMEDETECT_TYPES = PROP_PREFIX+".renamedetection.types";

	//apply diff props
	static final String PROP_DO_APPLYDIFF = PROP_PREFIX+".doapplydiff";
	static final String PROP_APPLYDIFF_TOSOURCE = PROP_PREFIX+".applydiff.tosource";
	static final String PROP_APPLYDIFF_TOCONN = PROP_PREFIX+".applydiff.toconn";
	static final String PROP_APPLYDIFF_TOID = PROP_PREFIX+".applydiff.toid";
	static final String PROP_APPLYDIFF_VALIDATE = PROP_PREFIX+".applydiff.validate";
	static final String PROP_APPLYDIFF_SCHEMADIFF = PROP_PREFIX+".doapplyschemadiff";
	static final String PROP_APPLYDIFF_DATADIFF = PROP_PREFIX+".doapplydatadiff";
	static final String PROP_APPLYDIFF_OBJECTTYPES = PROP_PREFIX+".applydiff.objecttypes";
	static final String PROP_APPLYDIFF_CHANGETYPES = PROP_PREFIX+".applydiff.changetypes";
	
	public static final boolean DEFAULT_DO_RENAME_DETECTION = false; //XXX: should be true?
	
	//props from SchemaModelScriptDumper
	@Deprecated static final String FILENAME_PATTERN_SCHEMA = "${schemaname}";
	@Deprecated static final String FILENAME_PATTERN_OBJECTTYPE = "${objecttype}";
	
	static final double RENAMEDETECT_MINSIMILARITY_DEFAULT = 0.5;
	
	static final String XML_IO_CLASS = "tbrugz.sqldiff.io.XMLDiffIO";
	static final String JSON_IO_CLASS = "tbrugz.sqldiff.io.JSONDiffIO";
	static final String PATCH_DUMPER_CLASS = "tbrugz.sqldiff.patch.PatchDumper";

	static final Log log = LogFactory.getLog(SQLDiff.class);
	
	final Properties prop = new ParametrizedProperties();

	boolean failonerror = true;
	String outfilePattern = null;
	
	String xmlinfile = null;
	String xmloutfile = null;

	String jsoninfile = null;
	String jsonoutfile = null;

	String patchfilePattern = null;
	
	boolean doApplyDiff = false;
	
	// writers
	CategorizedOut categOut = null; 
	Writer xmlWriter = null;
	Writer jsonWriter = null;
	Writer patchWriter = null;
	
	transient int lastDiffCount = 0;
	
	class ModelGrabber implements Callable<SchemaModel> {
		
		final SchemaModelGrabber schemaGrabber;
		final String grabberMode;
		final String grabberId;
		
		public ModelGrabber(SchemaModelGrabber grabber, String grabberMode, String grabberId) {
			this.grabberMode = grabberMode;
			this.grabberId = grabberId;
			this.schemaGrabber = grabber;
		}
		
		/*public ModelGrabber(String grabberMode, String grabberId) throws ClassNotFoundException, SQLException, NamingException {
			this.grabberMode = grabberMode;
			this.grabberId = grabberId;
			this.schemaGrabber = initGrabber(grabberMode, grabberId, prop);
		}*/
		
		@Override
		public SchemaModel call() throws Exception {
			try {
				//get grabber
				//this.schemaGrabber = initGrabber(grabberMode, grabberId, prop);
				
				//grab schemas
				log.info("grabbing '"+grabberMode+"' model ["+grabberId+"]");
				return schemaGrabber.grabSchema();
			}
			catch(Exception e) {
				log.warn("ModelGrabber:: call[grabSchema]: "+e, e);
				throw e;
			}
		}
	}
	
	public int doIt() throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException, InterruptedException, ExecutionException {
		
		SchemaModelGrabber fromSchemaGrabber = null;
		SchemaModelGrabber toSchemaGrabber = null;
		SchemaModel fromSM = null;
		SchemaModel toSM = null;
		String sourceId = null;
		String targetId = null;
		
		SchemaDiff diff = null;
		
		String diffDialect = DBMSResources.DEFAULT_DBID;
		
		long initTime = System.currentTimeMillis();
		
		if(xmlinfile!=null) {
			DiffGrabber dg = (DiffGrabber) Utils.getClassInstance(XML_IO_CLASS);
			diff = (SchemaDiff) dg.grabDiff(new File(xmlinfile));
			if(diff.getSqlDialect()!=null) { diffDialect = diff.getSqlDialect(); }
			setupFeatures(diffDialect);
		}
		else if(jsoninfile!=null) {
			DiffGrabber dg = (DiffGrabber) Utils.getClassInstance(JSON_IO_CLASS);
			diff = (SchemaDiff) dg.grabDiff(new File(jsoninfile));
			if(diff.getSqlDialect()!=null) { diffDialect = diff.getSqlDialect(); }
			setupFeatures(diffDialect);
		}
		else {
			//from
			sourceId = prop.getProperty(PROP_SOURCE);
			fromSchemaGrabber = initGrabber(ID_SOURCE, sourceId, prop);
			
			//to
			targetId = prop.getProperty(PROP_TARGET);
			toSchemaGrabber = initGrabber(ID_TARGET, targetId, prop);
			
			//XXX: add allowParallel property?
			// grab schemas - parallel execution
			ModelGrabber sourceGrabber = new ModelGrabber(fromSchemaGrabber, ID_SOURCE, sourceId);
			ModelGrabber targetGrabber = new ModelGrabber(toSchemaGrabber, ID_TARGET, targetId);
			
			ExecutorService executor = Executors.newFixedThreadPool(2);
			
			Future<SchemaModel> futureSourceSM = executor.submit(sourceGrabber);
			Future<SchemaModel> futureTargetSM = executor.submit(targetGrabber);
			executor.shutdown();
			
			fromSM = futureSourceSM.get(); //blocks for return
			toSM = futureTargetSM.get();
			
			/*// grab schemas - sequential
			log.info("grabbing 'source' model ["+sourceId+"]");
			fromSM = fromSchemaGrabber.grabSchema();
			log.info("grabbing 'target' model ["+targetId+"]");
			toSM = toSchemaGrabber.grabSchema();
			*/
			
			diff = doDiffSchemas(fromSM, toSM);
			diffDialect = diff.getSqlDialect();
			doDetectRenames(diff); //XXX: should detect renames even when using XML_IO_CLASS or JSON_IO_CLASS ?
		}
		
		//delete files from dir...
		String dirToDeleteFiles = prop.getProperty(PROP_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			DirectoryCleaner dc = new DirectoryCleaner();
			dc.setDirToDeleteFiles(new File(dirToDeleteFiles));
			dc.process();
		}

		//dump diff
		/*if(outfilePattern!=null) {
			String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern, 
					new String[]{FILENAME_PATTERN_SCHEMA, Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME)},
					new String[]{FILENAME_PATTERN_OBJECTTYPE, Defs.addSquareBraquets(Defs.PATTERN_OBJECTTYPE)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_OBJECTNAME)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE)}
					);
			CategorizedOut co = new CategorizedOut(finalPattern);
			log.debug("final pattern: "+finalPattern);
			
			//co.categorizedOut(diff.getDiff());
			//log.info("dumping diff...");
			diff.outDiffs(co);
		}*/
		//dump diff
		if(categOut!=null) {
			diff.outDiffs(categOut);
		}
		
		if(xmlWriter!=null) {
			try {
				DiffDumper dd = (DiffDumper) Utils.getClassInstance(XML_IO_CLASS);
				dd.setProperties(prop);
				dd.dumpDiff(diff, xmlWriter);
			} catch (JAXBException e) {
				log.warn("error writing xml: "+e);
				log.debug("error writing xml: "+e.getMessage(),e);
			}
		}

		if(jsonWriter!=null) {
			try {
				DiffDumper dd = (DiffDumper) Utils.getClassInstance(JSON_IO_CLASS);
				dd.setProperties(prop);
				dd.dumpDiff(diff, jsonWriter);
			} catch (JAXBException e) {
				log.warn("error writing json: "+e);
				log.debug("error writing json: "+e.getMessage(),e);
			}
		}
		
		//out patch - show changed lines, ... ; dbiddiff: replace changetype
		if(patchWriter!=null) {
			if(!SchemaDiffer.mayReplaceDbId) {
				log.warn("using PatchDumper with '"+PROP_DBIDDIFF_USEREPLACE+"'=="+SchemaDiffer.mayReplaceDbId+": duplicate diffs may appear");
			}
			
			DiffDumper dd = (DiffDumper) Utils.getClassInstance(PATCH_DUMPER_CLASS);
			dd.setProperties(prop);
			dd.dumpDiff(diff, patchWriter);
		}

		boolean doDataDiff = Utils.getPropBool(prop, PROP_DO_DATADIFF, false);
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
			boolean connectionCreated = false;
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
					connectionCreated = true;
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
					connectionCreated = true;
				}
			}
			else if(applyToConnPrefix!=null) {
				log.info("initting connection to apply diff [prefix = '"+applyToConnPrefix+"']");
				applyToConn = ConnectionUtil.initDBConnection(applyToConnPrefix, prop);
				connectionCreated = true;
			}
			else {
				String message = "applydiff (ditt-to-db) target (prop '"+PROP_APPLYDIFF_TOSOURCE+"' or '"+PROP_APPLYDIFF_TOCONN+"') not defined";
				log.warn(message);
				if(failonerror) { throw new ProcessingException(message); }
			}
			
			applySchemaDiff(diff, applyToModel, applyToConn, doApplyValidate);
			
			if(connectionCreated) { ConnectionUtil.closeConnection(applyToConn); }
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
		
		if(fromSchemaGrabber!=null) {
			ConnectionUtil.closeConnection(fromSchemaGrabber.getConnection());
		}
		if(toSchemaGrabber!=null) {
			ConnectionUtil.closeConnection(toSchemaGrabber.getConnection());
		}
		
		log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		
		return diff.getDiffList().size();
	}

	/*static void setupFeaturesDefault() {
		setupFeatures(DBMSResources.DEFAULT_DBID);
	}*/
	
	static void setupFeatures(String dialect) {
		log.debug("diff dialect set to: "+dialect);
		//DBMSResources.instance().updateDbId(dialect);
		DBMSFeatures feat = DBMSResources.instance().getSpecificFeatures(dialect);
		ColumnDiff.updateFeatures(feat);
	}

	static void setupFeaturesIfNull(String dialect) {
		if(ColumnDiff.isFeaturesNull()) {
			log.debug("setupFeaturesIfNull: diff dialect will be set to: "+dialect);
			setupFeatures(dialect);
		}
	}
	
	SchemaDiff doDiffSchemas(SchemaModel fromSM, SchemaModel toSM) throws ClassNotFoundException, SQLException, NamingException {
		//XXX: option to set dialect from properties?
		String dialect = fromSM.getSqlDialect();
		setupFeatures(dialect);
		
		// sql dialect transformer
		boolean doTransformTargetDialectToSource = Utils.getPropBool(prop, PROP_SCHEMADIFF_TARGET_DIALECT_TO_SOURCE, false);
		if(doTransformTargetDialectToSource) {
			log.info("will 'transform-target-dialect-to-source' [dialect="+dialect+"]");
			Processor sdt = new SQLDialectTransformer();
			Properties p = new Properties();
			p.setProperty(SQLDialectTransformer.PROP_TRANSFORM_TO_DBID, dialect);
			sdt.setProperties(p);
			sdt.setSchemaModel(toSM);
			sdt.process();
		}
		
		// source/target processors
		String sourceProcs = prop.getProperty(PROP_SCHEMADIFF_SOURCE_PROCESSORS);
		if(sourceProcs!=null) {
			processProcessors(sourceProcs, fromSM, "souce");
		}
		String targetProcs = prop.getProperty(PROP_SCHEMADIFF_TARGET_PROCESSORS);
		if(targetProcs!=null) {
			processProcessors(targetProcs, toSM, "target");
		}
		
		//do diff
		log.info("diffing... [dialect="+dialect+"]");
		SchemaDiffer differ = new SchemaDiffer();
		differ.setTypesForDiff(prop.getProperty(PROP_TYPES_TO_DIFF));
		return differ.diffSchemas(fromSM, toSM);
	}
	
	void processProcessors(String processorClassesStr, SchemaModel sm, String modelId) throws ClassNotFoundException, SQLException, NamingException {
		List<ProcessComponent> pcs = SQLDump.getProcessComponentClasses(processorClassesStr, failonerror);
		for(ProcessComponent pc: pcs) {
			if(pc instanceof Processor) {
				log.debug(modelId+"processor '"+pc+"' running...");
				Processor proc = (Processor) pc;
				pc.setProperties(prop);
				if(proc.needsConnection()) {
					log.warn(modelId+": processor '"+pc+"' needs connection?");
				}
				if(proc.needsSchemaModel()) {
					proc.setSchemaModel(sm);
				}
				proc.setFailOnError(failonerror);
				if(proc.acceptsOutputWriter()) {
					log.warn(modelId+"processor '"+pc+"' needs writer?");
				}
				proc.process();
			}
			else {
				log.warn(pc+": unknown processor type");
			}
		}
	}
	
	void doDetectRenames(SchemaDiff diff) {
		//detect renames
		//XXX: add DiffProcessor?
		//XXX: add prop 'sqldiff.renamedetection.types'?
		boolean doRenameDetection = Utils.getPropBool(prop, PROP_DO_RENAMEDETECTION, DEFAULT_DO_RENAME_DETECTION);
		
		if(doRenameDetection) {
			double minSimilarity = Utils.getPropDouble(prop, PROP_RENAMEDETECT_MINSIMILARITY, RENAMEDETECT_MINSIMILARITY_DEFAULT);
			int renames = 0;
			List<String> renameTypes = Utils.getStringListFromProp(prop, PROP_RENAMEDETECT_TYPES, ",", RenameDetector.RENAME_TYPES); // DBObjectType.values() ?
			
			if(renameTypes!=null) {
				log.info("types to detect renames: "+renameTypes);
			}
			
			if(renameTypes==null || renameTypes.contains(DBObjectType.TABLE.toString())) {
				renames += RenameDetector.detectAndDoTableRenames(diff.getTableDiffs(), minSimilarity);
			}
			if(renameTypes==null || renameTypes.contains(DBObjectType.COLUMN.toString())) {
				renames += RenameDetector.detectAndDoColumnRenames(diff.getColumnDiffs(), minSimilarity);
			}
			if(renameTypes==null || renameTypes.contains(DBObjectType.INDEX.toString())) {
				renames += RenameDetector.detectAndDoIndexRenames(diff.getDbIdDiffs(), minSimilarity);
			}
			if(renameTypes==null || renameTypes.contains(DBObjectType.CONSTRAINT.toString())) {
				renames += RenameDetector.detectAndDoConstraintRenames(diff.getDbIdDiffs(), minSimilarity);
			}
			//XXX detect FK renames?
			if(renames>0) {
				SchemaDiff.logInfo(diff);
			}
		}
	}

	void applySchemaDiff(SchemaDiff diff, SchemaModel applyToModel, Connection applyToConn, boolean doApplyValidate) throws SQLException {
		if(applyToConn==null) {
			String message = "applySchemaDiff: connection is null!";
			log.warn(message);
			if(failonerror) { throw new ProcessingException(message); }
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
			//DBMSResources.instance().updateMetaData(applyToConn.getMetaData());
			applyDiffToDB(diff, applyToConn);
		}
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
		if(Utils.isNullOrEmpty(grabberId)) { 
			throw new ProcessingException("'"+grabberLabel+"' grabber id not defined");
		}
		
		log.info(grabberLabel+" model ["+grabberId+"] init");
		String grabberPrefix = "sqldiff."+grabberId;
		String connPropPrefix = prop.getProperty(grabberPrefix+".connpropprefix", grabberPrefix);
		
		String grabClassName = prop.getProperty(grabberPrefix+".grabclass", prop.getProperty(connPropPrefix+".grabclass"));
		if(grabClassName==null) {
			throw new ProcessingException("'"+grabberLabel+"' grabber class not defined [id="+grabberId+" ; prefix="+connPropPrefix+"]");
		}
		
		SchemaModelGrabber schemaGrabber = initSchemaModelGrabberInstance(grabClassName);
		schemaGrabber.setId(grabberId);
		schemaGrabber.setPropertiesPrefix(connPropPrefix);
		schemaGrabber.setProperties(prop);
		if(schemaGrabber.needsConnection()) {
			Connection conn = ConnectionUtil.initDBConnection(connPropPrefix, prop);
			schemaGrabber.setConnection(conn);
		}
		return schemaGrabber;
	}
	
	@SuppressWarnings("deprecation")
	public void procProterties() {
		failonerror = Utils.getPropBool(prop, PROP_FAILONERROR, failonerror);
		DBObject.dumpCreateOrReplace = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_USECREATEORREPLACE, false);
		SQLIdentifierDecorator.dumpQuoteAll = Utils.getPropBool(prop, SchemaModelScriptDumper.PROP_SCHEMADUMP_QUOTEALLSQLIDENTIFIERS, SQLIdentifierDecorator.dumpQuoteAll);
		
		outfilePattern = prop.getProperty(PROP_OUTFILEPATTERN);
		xmlinfile = prop.getProperty(PROP_XMLINFILE);
		xmloutfile = prop.getProperty(PROP_XMLOUTFILE);
		jsoninfile = prop.getProperty(PROP_JSONINFILE);
		jsonoutfile = prop.getProperty(PROP_JSONOUTFILE);
		patchfilePattern = prop.getProperty(PROP_PATCHFILEPATTERN);
		doApplyDiff = Utils.getPropBool(prop, PROP_DO_APPLYDIFF, doApplyDiff);
		
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
		SchemaDiffer.mayReplaceDbId = Utils.getPropBool(prop, PROP_DBIDDIFF_USEREPLACE, SchemaDiffer.mayReplaceDbId);
		DBMSResources.instance().setup(prop);
	}
	
	void openFileWriters() throws IOException {
		if(outfilePattern!=null) {
			String finalPattern = CategorizedOut.generateFinalOutPattern(outfilePattern, 
					new String[]{FILENAME_PATTERN_SCHEMA, Defs.addSquareBraquets(Defs.PATTERN_SCHEMANAME)},
					new String[]{FILENAME_PATTERN_OBJECTTYPE, Defs.addSquareBraquets(Defs.PATTERN_OBJECTTYPE)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_OBJECTNAME)},
					new String[]{Defs.addSquareBraquets(Defs.PATTERN_CHANGETYPE)}
					);
			CategorizedOut co = new CategorizedOut(finalPattern);
			setCategorizedOut(co);
			log.debug("final pattern: "+finalPattern);
		}
		
		if(xmloutfile!=null) {
			File f = new File(xmloutfile);
			Utils.prepareDir(f);
			xmlWriter = new FileWriter(f);
		}
		if(jsonoutfile!=null) {
			File f = new File(jsonoutfile);
			Utils.prepareDir(f);
			jsonWriter = new FileWriter(f);
		}
		if(patchfilePattern!=null) {
			File f = new File(patchfilePattern);
			Utils.prepareDir(f);
			patchWriter = new FileWriter(f);
		}
	}
	
	void applyDiffToDB(SchemaDiff diff, Connection conn) throws SQLException {
		//do not apply DDLs with comments
		boolean savedAddComments = ColumnDiff.addComments;
		ColumnDiff.addComments = false;
		
		List<Diff> diffs = diff.getChildren();

		int diffCount = 0;
		int execCount = 0;
		int errorCount = 0;
		int updateCount = 0;
		int skipCount = 0;
		
		SQLException lastEx = null;
		List<String> allowedObjectTypes = Utils.getStringListFromProp(prop, PROP_APPLYDIFF_OBJECTTYPES, ",");
		List<String> allowedChangeTypes = Utils.getStringListFromProp(prop, PROP_APPLYDIFF_CHANGETYPES, ",");
		
		for(Diff d: diffs) {
			diffCount++;
			if(allowedObjectTypes!=null && !allowedObjectTypes.contains(d.getObjectType().toString())) {
				log.info("diff #"+diffCount+": '"+d.getNamedObject()+"'s objectType ["+d.getObjectType()+"] not in '"+allowedObjectTypes+"'");
				skipCount++;
				continue;
			}

			if(allowedChangeTypes!=null && !allowedChangeTypes.contains(d.getChangeType().toString())) {
				log.info("diff #"+diffCount+": '"+d.getNamedObject()+"'s changeType ["+d.getChangeType()+"] not in '"+allowedChangeTypes+"'");
				skipCount++;
				continue;
			}
			
			//sqldiff.applydiff.DROP=COLUMN, TABLE, ...
			List<String> types = Utils.getStringListFromProp(prop, "sqldiff.applydiff."+d.getChangeType(), ",");
			if(types!=null && !types.contains(d.getObjectType().toString())) {
				log.info("diff #"+diffCount+": '"+d.getNamedObject()+"'s type ["+d.getObjectType()+"] not in '"+d.getChangeType()+"' types: "+types);
				skipCount++;
				continue;
			}
			
			try {
				//XXX: option to send all SQLs from one diff in only one statement? no problem for h2...  
				List<String> sqls = d.getDiffList();
				if(sqls.size()==0) {
					log.info("diff #"+diffCount+": no SQL diff avaiable for "+d.getChangeType()+" on '"+d.getNamedObject()+"': "+d);
				}
				else {
				for(int i=0;i<sqls.size();i++) {
					String sql = sqls.get(i);
					log.info("executing diff #"+diffCount
						+(sqls.size()>1?" ["+(i+1)+"/"+sqls.size()+"]: ":" ")
						+("[ "+d.getChangeType()+": "+d.getNamedObject()+" ]: ")
						+sql);
					execCount++;
					updateCount += conn.createStatement().executeUpdate(sql);
					String previousDef = d.getPreviousDefinition();
					if(!Utils.isNullOrEmpty(previousDef) && log.isDebugEnabled()) {
						log.debug("diff #"+diffCount+":\n- previous= "+previousDef+"\n- new     = "+d.getDefinition());
					}
				}
				}
			} catch (SQLException e) {
				errorCount++;
				lastEx = e;
				log.warn("error executing diff: "+e+"\n- previous= "+d.getPreviousDefinition()+"\n- new     = "+d.getDefinition());
				if(failonerror) { break; }
			}
		}
		log.info(
				(execCount>0?execCount:"no")
				+" diff statements executed"
				+(errorCount>0?" [#errors = "+errorCount+"]":"")
				+(updateCount>0?" [#updates = "+updateCount+"]":"")
				+(skipCount>0?" [#skiped = "+skipCount+"]":"")
				);

		ColumnDiff.addComments = savedAddComments;
		
		if(execCount>0 && errorCount==0) {
			if(conn.getAutoCommit()) {
				log.info("committed "+execCount+" changes... [autocommit enabled]");
			}
			else {
				log.info("committing "+execCount+" changes...");
				conn.commit();
			}
		}
		
		if(failonerror && errorCount>0) {
			throw new ProcessingException(errorCount+" sqlExceptions occured", lastEx);
		}
	}

	@Override
	public void doMain(String[] args, Properties properties) throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException, InterruptedException, ExecutionException {
		if(properties!=null) {
			prop.putAll(properties);
		}
		if(CLIProcessor.shouldStopExec(PRODUCT_NAME, args)) {
			return;
		}
		CLIProcessor.init(PRODUCT_NAME, args, PROPERTIES_FILENAME, prop);
		
		procProterties();

		if(outfilePattern==null && xmloutfile==null && jsonoutfile==null && !doApplyDiff) {
			String message = "outfilepattern [prop '"+PROP_OUTFILEPATTERN+"']"+
					" nor xmloutfile [prop '"+PROP_XMLOUTFILE+"']"+
					" nor jsonoutfile [prop '"+PROP_JSONOUTFILE+"']"+
					" nor doapplydiff [prop '"+PROP_DO_APPLYDIFF+"'] defined. can't proceed";
			log.error(message);
			if(failonerror) { throw new ProcessingException(message); }
			return;
		}
		
		openFileWriters();
		
		lastDiffCount = doIt();
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException, JAXBException, XMLStreamException, InterruptedException, ExecutionException {
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
	
	public void setProperties(Properties properties) {
		if(properties!=null) {
			prop.putAll(properties);
		}
	}
	
	public void setCategorizedOut(CategorizedOut out) {
		categOut = out;
	}
	
	public void setXmlWriter(Writer writer) {
		xmlWriter = writer;
	}

	public void setJsonWriter(Writer writer) {
		jsonWriter = writer;
	}

	public void setPatchWriter(Writer writer) {
		patchWriter = writer;
	}

}
