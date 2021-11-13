package tbrugz.sqldump;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DumpSyntaxRegistry;
import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.Executor;
import tbrugz.sqldump.def.ProcessComponent;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.jmx.SQLD;
import tbrugz.sqldump.processors.DirectoryCleaner;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.JMXUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;

/*
 * XXX(later): usePrecision should be defined by java code (not .properties)
 * !TODO: include demo schema and data
 * XXX(later): generate schema model from graphML file (XMLUnit?). may be used for model comparison 
 * XXX: new grabber: scriptGrabber - antlr?
 * XXX!: new dumper: test case dumper: dumps defined records and its parent/child records based on FKs (needs schema and connection)
 * XXX: add shutdown option (Derby). see JDBCSchemaGrabber.grabDbSpecificFeaturesClass()
 * XXX: add startup option, before opening connection (SQLite, ...) - readOnlyConnection , ...
 * TODO: more transparent way of selecting index grabbing strategy: 'sqldump.dbspecificfeatures.grabindexes' / 'sqldump.doschemadump.indexes'
 * XXX: FK 'on delete cascade'? UNIQUE constraints 'not null'? other modifiers?
 * ~XXX: create view WITH CHECK OPTION - can only update rows thar are accessible through the view (+ WITH READ ONLY)
 * XXX: add junit tests for all "supported" databases (needs sqlregen first?)
 * XXX?: option for queries to have specific syntax-dumpers
 * XXXdone: option for specific queries to have specific syntax-dumpers
 * TODO!: filter tables/executables/trigger (/index/view/mv/sequence ?) by name (include only/exclude)
 * TODO: output ffc with optional trimming
 * TODO: use sql quote when names are equal to sql keywords or have invalid characters (" ", "-", ...) - SchemaModelScriptDumper, AlterSchemaSuggestion
 * ~TODO: sqldump.schemagrab.xtratables=<schema>.<table>, <table2>
 */
public class SQLDump implements Executor {
	
	public static final String CONN_PROPS_PREFIX = "sqldump"; //XXX: change to non-public
	public static final String PRODUCT_NAME = "sqldump";
	
	//sqldump.properties
	static final String PROP_GRABCLASS = "sqldump.grabclass";
	@Deprecated static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass"; // sqldump.grabclass?
	@Deprecated static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses"; // sqldump.dumpclasses?
	static final String PROP_PROCESSINGCLASSES = "sqldump.processingclasses";  //.(pre)processors?
	@Deprecated static final String PROP_PROCESSINGCLASSES_AFTERDUMPERS = "sqldump.processingclasses.afterdumpers";  //.postprocessors?

	public static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	public static final String PROP_CONNPROPPREFIX = "sqldump.connpropprefix"; //XXX: change to non-public
	static final String PROP_FAILONERROR = "sqldump.failonerror";
	static final String PROP_JMX_CREATE_MBEAN = "sqldump.jmx.create-mbean";
	static final String PROP_DATADUMP_XTRASYNTAXES = "sqldump.datadump.xtrasyntaxes";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	
	static final Log log = LogFactory.getLog(SQLDump.class);
	
	Connection conn;
	boolean connCreated = false;
	boolean failonerror = true;
	boolean jmxCreateMBean = false;

	final Properties papp = new ParametrizedProperties();
	
	void init(String[] args) throws IOException {
		CLIProcessor.init(PRODUCT_NAME, args, PROPERTIES_FILENAME, papp); //generic arguments init
		
		failonerror = Utils.getPropBool(papp, PROP_FAILONERROR, failonerror);
		log.info("failonerror: "+failonerror);

		jmxCreateMBean = Utils.getPropBool(papp, PROP_JMX_CREATE_MBEAN, jmxCreateMBean);
		log.debug("jmx.create-mbean: "+jmxCreateMBean);
		
		ColTypeUtil.setProperties(papp);
		DBMSResources.instance().setup(papp);
		SQLUtils.setProperties(papp);
		DumpSyntaxRegistry.addSyntaxes(papp.getProperty(PROP_DATADUMP_XTRASYNTAXES));
	}

	/*void end(boolean closeConnection) throws SQLException {
		if(closeConnection) {
			ConnectionUtil.closeConnection(conn);
		}
	}*/
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDump sdd = new SQLDump();
		sdd.doMain(args, null, null);
	}

	@Override
	public void doMain(String[] args, Properties prop) throws ClassNotFoundException, SQLException, NamingException, IOException {
		doMain(args, prop, null);
	}
	
	public void doMain(String[] args, Properties prop, Connection c) throws ClassNotFoundException, SQLException, NamingException, IOException {
		doMain(args, prop, c, null);
	}
	
	public void doMain(String[] args, Properties prop, Connection c, Writer writer) throws ClassNotFoundException, SQLException, NamingException, IOException {
		if(CLIProcessor.shouldStopExec(PRODUCT_NAME, args)) {
			return;
		}
		
		if(c!=null) { conn = c; }
		
		long initTime = System.currentTimeMillis();
		papp.setProperty(Defs.PROP_START_TIME_MILLIS, String.valueOf(initTime));
		
		try {

		if(prop!=null) {
			papp.putAll(prop);
		}

		init(args);
		
		//Utils.showSysProperties();
		
		SchemaModelGrabber schemaGrabber = null;
		List<ProcessComponent> processors = new ArrayList<ProcessComponent>();
		
		//class names
		String grabClassName = Utils.getPropWithDeprecated(papp, PROP_GRABCLASS, PROP_SCHEMAGRAB_GRABCLASS, null);
		String processingClassesStr = papp.getProperty(PROP_PROCESSINGCLASSES);
		String dumpSchemaClasses = papp.getProperty(PROP_SCHEMADUMP_DUMPCLASSES);
		String processingClassesAfterDumpersStr = papp.getProperty(PROP_PROCESSINGCLASSES_AFTERDUMPERS);
		
		if(dumpSchemaClasses!=null) {
			log.warn("property '"+PROP_SCHEMADUMP_DUMPCLASSES+"' is deprecated. You should probably add the dumpclasses to '"+PROP_PROCESSINGCLASSES+"' property");
		}
		if(processingClassesAfterDumpersStr!=null) {
			log.warn("property '"+PROP_PROCESSINGCLASSES_AFTERDUMPERS+"' is deprecated. You should probably add the after-dump processingclasses to '"+PROP_PROCESSINGCLASSES+"' property");
		}
		
		if(grabClassName!=null && dumpSchemaClasses==null && processingClassesStr==null) {
			log.warn("grabber class [prop '"+PROP_GRABCLASS+"'] defined but no dumper [deprecated prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] or processing [prop '"+PROP_PROCESSINGCLASSES+"'] classes defined");
			//XXX: throw ProcessingException ?
		}
		if(grabClassName==null && dumpSchemaClasses!=null) {
			log.warn("dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined but no grab class [prop '"+PROP_GRABCLASS+"'] defined");
		}
		if(grabClassName==null && dumpSchemaClasses==null && processingClassesStr==null) {
			String message = "no grabber [prop '"+PROP_GRABCLASS+"'], dumper [deprecated prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] or processing [prop '"+PROP_PROCESSINGCLASSES+"'] classes defined";
			log.error(message);
			if(failonerror) { throw new ProcessingException(message); }
		}
		
		//grabbing model
		if(grabClassName!=null) {
			grabClassName = grabClassName.trim();
			schemaGrabber = (SchemaModelGrabber) Utils.getClassInstance(grabClassName, Defs.DEFAULT_CLASSLOADING_PACKAGES);
			if(schemaGrabber==null) {
				log.error("schema grabber class '"+grabClassName+"' not found");
			}
		}
		else {
			log.debug("no schema grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		
		//processor: DirectoryCleaner (deleteDirContents)
		String dirToDeleteFiles = papp.getProperty(PROP_DO_DELETEREGULARFILESDIR);
		if(dirToDeleteFiles!=null) {
			DirectoryCleaner delDirProc = new DirectoryCleaner();
			delDirProc.setDirToDeleteFiles(new File(dirToDeleteFiles));
			processors.add(delDirProc);
		}
		
		//processing classes
		if(processingClassesStr!=null) {
			processors.addAll(getProcessComponentClasses(processingClassesStr, failonerror));
		}
		
		//dumping model
		if(dumpSchemaClasses!=null) {
			String[] dumpClasses = dumpSchemaClasses.split(",");
			for(String dumpClass: dumpClasses) {
				dumpClass = dumpClass.trim();
				SchemaModelDumper schemaDumper = (SchemaModelDumper) Utils.getClassInstance(dumpClass, Defs.DEFAULT_CLASSLOADING_PACKAGES);
				if(schemaDumper!=null) {
					processors.add(schemaDumper);
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
			processors.addAll(getProcessComponentClasses(processingClassesAfterDumpersStr, failonerror));
		}
		
		doMainProcess(schemaGrabber, processors, writer);
		
		}
		//XXX: error-processors (like SendMail)?
		finally {
			if(connCreated) {
				ConnectionUtil.closeConnection(conn);
			}
			log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		}
	}
	
	void doMainProcess(SchemaModelGrabber grabber, List<ProcessComponent> processors, Writer writer) throws ClassNotFoundException, SQLException, NamingException {

		int numOfProcessors = processors.size();
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<numOfProcessors;i++) {
			ProcessComponent pc = processors.get(i);
			sb.append((i>0?", ":"")+pc.getClass().getSimpleName());
		}
		if(grabber!=null) {
			log.info("sqldump grabber: "+grabber.getClass().getSimpleName());
			processors.add(0, grabber);
		}
		log.info("sqldump processors [#"+numOfProcessors+"]: "+sb.toString());
		int numOfComponents = processors.size();

		//jmx
		SQLD sqldmbean = null;
		if(jmxCreateMBean) {
			sqldmbean = new SQLD(numOfComponents, (conn!=null)?conn.getMetaData():null );
			JMXUtil.registerMBeanSimple(SQLD.MBEAN_NAME, sqldmbean);
		}
		
		int count = 0;
		SchemaModel sm = null;
		
		//inits DBMSFeatures if not already initted
		//DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass(); //XXXxx: really needed?
		//log.debug("DBMSFeatures: "+feats);
		
		for(ProcessComponent pc: processors) {
			count++;
			log.debug("processor '"+pc.getClass().getSimpleName()+"' starter ["+count+"/"+numOfComponents+"]");
			
			if(sqldmbean!=null) {
				sqldmbean.newTaskUpdate(count, String.valueOf(count), pc.getClass().getSimpleName(), "");
			}
			
			if((count==1) && (pc instanceof SchemaModelGrabber)) {
				sm = doProcessGrabber((SchemaModelGrabber)pc);
			}
			else if(pc instanceof SchemaModelDumper) {
				doProcessDumper((SchemaModelDumper)pc, sm, writer);
			}
			else if(pc instanceof Processor) {
				Connection newConn = doProcessProcessor((Processor)pc, sm, writer);
				if(newConn!=null) {
					conn = newConn;
				}
			}
			else {
				log.warn("unknown processor type: "+pc.getClass().getName());
				if(failonerror) {
					throw new ProcessingException("unknown processor type: "+pc.getClass().getName());
				}
			}
			
			if(sqldmbean!=null && conn!=null) {
				sqldmbean.dbmdUpdate(conn.getMetaData());
			}
			
			log.debug("processor '"+pc.getClass().getSimpleName()+"' ended ["+count+"/"+numOfComponents+"]");
		}
	}
	
	SchemaModel doProcessGrabber(SchemaModelGrabber schemaGrabber) throws ClassNotFoundException, SQLException, NamingException {
		SchemaModel sm = null;
		
		if(schemaGrabber!=null) {
			schemaGrabber.setProperties(papp);
			if(schemaGrabber.needsConnection()) {
				if(conn==null) { setupConnection(); }
				schemaGrabber.setConnection(conn);
				schemaGrabber.setFailOnError(failonerror);
			}
			sm = schemaGrabber.grabSchema();
		}
		
		if(sm!=null) {
			//DBMSResources.instance().updateDbId(sm.getSqlDialect()); //XXXxx: really needed?
		}
		else {
			log.warn("no model grabbed!");
		}
		return sm;
	}
	
	/**
	 * Executes the processor
	 * 
	 * @return a new connection (if the processor has such capability; if it does not, returns null)
	 */
	Connection doProcessProcessor(Processor sqlproc, SchemaModel sm, Writer writer) throws ClassNotFoundException, SQLException, NamingException {
		sqlproc.setProperties(papp);
		if(sqlproc.needsConnection()) {
			if(conn==null) {
				setupConnection();
			}
			sqlproc.setConnection(conn);
		}
		if(sqlproc.needsSchemaModel()) {
			sqlproc.setSchemaModel(sm);
		}
		//TODO: set fail on error based on (processor) properties ?
		sqlproc.setFailOnError(failonerror);
		if(writer!=null && sqlproc.acceptsOutputWriter()) {
			sqlproc.setOutputWriter(writer);
		}
		sqlproc.process();
		return sqlproc.getNewConnection();
	}
	
	void doProcessDumper(SchemaModelDumper schemaDumper, SchemaModel sm, Writer writer) {
		schemaDumper.setProperties(papp);
		schemaDumper.setFailOnError(failonerror);
		if(writer!=null && schemaDumper.acceptsOutputWriter()) {
			schemaDumper.setOutputWriter(writer);
		}
		schemaDumper.dumpSchema(sm);
	}
	
	void setupConnection() throws ClassNotFoundException, SQLException, NamingException {
		String connPrefix = papp.getProperty(PROP_CONNPROPPREFIX);
		if(connPrefix==null) {
			connPrefix = CONN_PROPS_PREFIX;
		}
		else {
			log.info("connection properties prefix: '"+connPrefix+"'");
		}
		conn = ConnectionUtil.initDBConnection(connPrefix, papp);
		ConnectionUtil.showDBInfo(conn.getMetaData());
		if(conn==null) {
			if(failonerror) {
				throw new ProcessingException("can't init connection [prefix="+connPrefix+"]");
			}
		}
		else {
			connCreated = true;
			//DBMSResources.instance().updateMetaData(conn.getMetaData(), true); //XXXxx: really needed?
		}
	}
	
	public static List<ProcessComponent> getProcessComponentClasses(String processingClassesStr, boolean failonerror) throws ClassNotFoundException, SQLException, NamingException {
		List<ProcessComponent> processors = new ArrayList<ProcessComponent>();
		
		String[] processingClasses = processingClassesStr.split(",");
		for(String procClass: processingClasses) {
			procClass = procClass.trim();
			try {
				ProcessComponent sqlproc = (ProcessComponent) Utils.getClassInstance(procClass, Defs.DEFAULT_CLASSLOADING_PACKAGES);
				if(sqlproc!=null) {
					processors.add(sqlproc);
				}
				else {
					String message = "Error initializing processing class: '"+procClass+"'";
					log.error(message);
					if(failonerror) { throw new ProcessingException(message); }
				}
			}
			catch(Throwable e) {
				String message = "processor '"+procClass+"' error: "+e;
				log.warn(message);
				if(failonerror) { throw new ProcessingException(message, e); }
			}
		}
		return processors;
	}
	
	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}
	
}
