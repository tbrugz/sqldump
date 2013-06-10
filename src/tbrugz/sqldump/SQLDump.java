package tbrugz.sqldump;

import java.util.*;
import java.sql.*;
import java.io.*;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.dbmodel.SchemaModel;
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessComponent;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.def.Processor;
import tbrugz.sqldump.def.SchemaModelDumper;
import tbrugz.sqldump.def.SchemaModelGrabber;
import tbrugz.sqldump.jmx.SQLD;
import tbrugz.sqldump.util.CLIProcessor;
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
 * XXX: filter tables/executables/trigger (/index/view/mv/sequence ?) by name (include only/exclude)
 * TODO: output ffc with optional trimming
 * TODO: use sql quote when names are equal to sql keywords or have invalid characters (" ", "-", ...) - SchemaModelScriptDumper, AlterSchemaSuggestion
 * ~TODO: sqldump.schemagrab.xtratables=<schema>.<table>, <table2>
 */
public class SQLDump {
	
	public static final String CONN_PROPS_PREFIX = "sqldump";
	
	//sqldump.properties
	public static final String PROP_SCHEMAGRAB_GRABCLASS = "sqldump.schemagrab.grabclass";
	public static final String PROP_SCHEMADUMP_DUMPCLASSES = "sqldump.schemadump.dumpclasses";
	public static final String PROP_PROCESSINGCLASSES = "sqldump.processingclasses";  //.(pre)processors?
	public static final String PROP_PROCESSINGCLASSES_AFTERDUMPERS = "sqldump.processingclasses.afterdumpers";  //.postprocessors?

	static final String PROP_DO_DELETEREGULARFILESDIR = "sqldump.deleteregularfilesfromdir";
	public static final String PROP_DUMPSCHEMAPATTERN = "sqldump.dumpschemapattern";
	public static final String PROP_CONNPROPPREFIX = "sqldump.connpropprefix";
	static final String PROP_FAILONERROR = "sqldump.failonerror";
	
	//properties files filenames
	static final String PROPERTIES_FILENAME = "sqldump.properties";
	
	static final Log log = LogFactory.getLog(SQLDump.class);
	
	Connection conn;
	boolean failonerror = true;

	final Properties papp = new ParametrizedProperties();
	
	void init(String[] args) throws IOException {
		CLIProcessor.init("sqldump", args, PROPERTIES_FILENAME, papp); //generic arguments init
		
		failonerror = Utils.getPropBool(papp, PROP_FAILONERROR, failonerror);
		log.info("failonerror: "+failonerror);
		
		ColTypeUtil.setProperties(papp);
		DBMSResources.instance().setup(papp);
	}

	void end(boolean closeConnection) throws SQLException {
		if(closeConnection) {
			SQLUtils.ConnectionUtil.closeConnection(conn);
		}
	}
	
	/**
	 * @param args
	 * @throws NamingException 
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, NamingException, IOException {
		SQLDump sdd = new SQLDump();
		sdd.doMain(args, null, null);
	}

	public void doMain(String[] args, Properties prop) throws ClassNotFoundException, SQLException, NamingException, IOException {
		doMain(args, prop, null);
	}
	
	public void doMain(String[] args, Properties prop, Connection c) throws ClassNotFoundException, SQLException, NamingException, IOException {
		if(c!=null) { conn = c; }
		
		long initTime = System.currentTimeMillis();
		
		try {

		init(args);
		
		if(prop!=null) {
			papp.putAll(prop);
		}
		
		//Utils.showSysProperties();
		
		SchemaModel sm = null;
		SchemaModelGrabber schemaGrabber = null;
		List<ProcessComponent> processors = new ArrayList<ProcessComponent>();
		//DBMSResources.instance().updateMetaData(null);
		
		//class names
		String grabClassName = papp.getProperty(PROP_SCHEMAGRAB_GRABCLASS);
		String processingClassesStr = papp.getProperty(PROP_PROCESSINGCLASSES);
		String dumpSchemaClasses = papp.getProperty(PROP_SCHEMADUMP_DUMPCLASSES);
		String processingClassesAfterDumpersStr = papp.getProperty(PROP_PROCESSINGCLASSES_AFTERDUMPERS);
		
		if(grabClassName!=null && dumpSchemaClasses==null && processingClassesStr==null) {
			log.warn("grabber class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined but no dumper [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] or processing [prop '"+PROP_PROCESSINGCLASSES+"'] classes defined");
			//XXX: throw ProcessingException ?
		}
		if(grabClassName==null && dumpSchemaClasses!=null) {
			log.warn("dumper classes [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] defined but no grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		if(grabClassName==null && dumpSchemaClasses==null && processingClassesStr==null) {
			String message = "no grabber [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'], dumper [prop '"+PROP_SCHEMADUMP_DUMPCLASSES+"'] or processing [prop '"+PROP_PROCESSINGCLASSES+"'] classes defined";
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
			else {
				processors.add(schemaGrabber);
			}
		}
		else {
			log.debug("no schema grab class [prop '"+PROP_SCHEMAGRAB_GRABCLASS+"'] defined");
		}
		
		//processing classes
		if(processingClassesStr!=null) {
			processors.addAll(getProcessComponentClasses(processingClassesStr, sm));
		}
		
		//dumping model
		if(dumpSchemaClasses!=null) {
			String dumpClasses[] = dumpSchemaClasses.split(",");
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
			processors.addAll(getProcessComponentClasses(processingClassesAfterDumpersStr, sm));
		}
		
		doMainProcess(processors);
		
		}
		finally {
			end(c==null);
			log.info("...done [elapsed="+(System.currentTimeMillis()-initTime)+"ms]");
		}
	}
	
	void doMainProcess(List<ProcessComponent> processors) throws ClassNotFoundException, SQLException, NamingException {

		int numOfComponents = processors.size();
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<numOfComponents;i++) {
			ProcessComponent pc = processors.get(i);
			sb.append((i>0?", ":"")+pc.getClass().getSimpleName());
		}
		log.info("sqldump processors [#"+numOfComponents+"]: "+sb.toString());

		//jmx
		SQLD sqldmbean = new SQLD(numOfComponents, (conn!=null)?conn.getMetaData():null );
		JMXUtil.registerMBeanSimple(SQLD.MBEAN_NAME, sqldmbean);
		
		int count = 0;
		SchemaModel sm = null;
		
		//XXX change 'dirtodelete' to processor?
		String dirToDeleteFiles = papp.getProperty(PROP_DO_DELETEREGULARFILESDIR);
		
		//inits DBMSFeatures if not already initted
		DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass(); //XXX: really needed?
		log.debug("DBMSFeatures: "+feats);
		
		for(ProcessComponent pc: processors) {
			count++;
			sqldmbean.newTaskUpdate(count, String.valueOf(count), pc.getClass().getSimpleName(), "");
			
			if((count==1) && (pc instanceof SchemaModelGrabber)) {
				sm = doProcessGrabber((SchemaModelGrabber)pc);
				if(dirToDeleteFiles!=null) {
					Utils.deleteDirRegularContents(dirToDeleteFiles);
					dirToDeleteFiles = null;
				}
			}
			else if(pc instanceof SchemaModelDumper) {
				doProcessDumper((SchemaModelDumper)pc, sm);
			}
			else if(pc instanceof Processor) {
				Connection newConn = doProcessProcessor((Processor)pc, sm);
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
			
			if(conn!=null) {
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
			DBMSResources.instance().updateDbId(sm.getSqlDialect());
		}
		else {
			log.warn("no model grabbed!");
		}
		return sm;
	}
	
	Connection doProcessProcessor(Processor sqlproc, SchemaModel sm) {
		sqlproc.setProperties(papp);
		sqlproc.setConnection(conn);
		sqlproc.setSchemaModel(sm);
		//TODO: set fail on error based on (processor) properties ?
		sqlproc.setFailOnError(failonerror);
		sqlproc.process();
		return sqlproc.getConnection();
	}
	
	void doProcessDumper(SchemaModelDumper schemaDumper, SchemaModel sm) {
		schemaDumper.setProperties(papp);
		schemaDumper.setFailOnError(failonerror);
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
		conn = SQLUtils.ConnectionUtil.initDBConnection(connPrefix, papp);
		if(conn==null) {
			if(failonerror) {
				throw new ProcessingException("can't init connection [prefix="+connPrefix+"]");
			}
		}
		else {
			DBMSResources.instance().updateMetaData(conn.getMetaData()); //XXX: really needed?
		}
	}
	
	List<ProcessComponent> getProcessComponentClasses(String processingClassesStr, SchemaModel sm) throws ClassNotFoundException, SQLException, NamingException {
		List<ProcessComponent> processors = new ArrayList<ProcessComponent>();
		
		if(conn==null) { setupConnection(); }
		
		String processingClasses[] = processingClassesStr.split(",");
		for(String procClass: processingClasses) {
			procClass = procClass.trim();
			ProcessComponent sqlproc = (ProcessComponent) Utils.getClassInstance(procClass, Defs.DEFAULT_CLASSLOADING_PACKAGES);
			if(sqlproc!=null) {
				processors.add(sqlproc);
			}
			else {
				log.error("Error initializing processing class: '"+procClass+"'");
			}
		}
		return processors;
	}
	
}
