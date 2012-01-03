package tbrugz.sqldump.sqlregen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import tbrugz.sqldump.ParametrizedProperties;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.Utils;

import static tbrugz.sqldump.SQLDump.*; 

/*
 * TODO: cli (sqlplus-like)? continue/exit on error?
 * XXXxx: show/log ordered 'exec-ids' before executing
 * XXX: option for numeric/alphanumeric proc-ids
 * TODO: one statement per file option
 * XXX: statements 'split-by' option 
 */
public class SQLRegen {
	
	static Logger log = Logger.getLogger(SQLRegen.class);
	
	static final String PROPERTIES_FILENAME = "sqlregen.properties";
	static final String CONN_PROPS_PREFIX = "sqlregen"; 
	
	//prefixes
	static final String PREFIX_EXEC = "sqlregen.exec.";

	//sufixes
	static String SUFFIX_FILE = ".file";
	static String SUFFIX_STATEMENT = ".statement";
	static String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	
	static String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_STATEMENT };
	
	Properties papp = new ParametrizedProperties();
	Connection conn;
	
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
		papp.load(new FileInputStream(propFile));
		
		File propFileDir = propFile.getAbsoluteFile().getParentFile();
		log.debug("propfile base dir: "+propFileDir);
		papp.setProperty(PROP_PROPFILEBASEDIR, propFileDir.toString());
	}

	void end() throws Exception {
		if(conn!=null) {
			log.info("closing connection: "+conn);
			conn.close();
		}
		log.info("...done");
	}
	
	void doIt() throws IOException, SQLException {
		List<String> execkeys = Utils.getKeysStartingWith(papp, PREFIX_EXEC);
		Collections.sort(execkeys);
		log.info("init processing...");
		long initTime = System.currentTimeMillis();

		Set<String> procIds = new TreeSet<String>();
		for(String key: execkeys) {
			String procId = getExecId(key, PREFIX_EXEC);
			procIds.add(procId);
		}
		//Collections.sort(procIds);
		log.info("processing ids in exec order: "+Utils.join(procIds, ", "));
		
		StmtProc srproc = new StmtProc();
		srproc.setConn(conn);
		srproc.setPapp(papp);
		//TODO: use procIds instead of execkeys (?)
		for(String key: execkeys) {
			String procId = getExecId(key, PREFIX_EXEC);
			// .file
			if(key.endsWith(SUFFIX_FILE)) {
				log.info(">>> processing: id = '"+procId+"'");
				//String execId = getExecId(key, PREFIX_EXEC, SUFFIX_FILE);
				//log.info("processing: exec-id = "+execId);
				try {
					srproc.execFile(key, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS);
				}
				catch(FileNotFoundException e) {
					log.warn("file not found: "+e);
					log.debug("file not found", e);
				}
			}
			// .statement
			else if(key.endsWith(SUFFIX_STATEMENT)) {
				log.info(">>> processing: id = '"+procId+"'");
				String stmtStr = papp.getProperty(key);
				try {
					int urows = srproc.execStatement(stmtStr);
				}
				catch(SQLException e) {
					log.warn("error executing statement [stmt = "+stmtStr+"]: "+e);
					log.debug("error executing statement", e);
				}
			}
			// ...else
			else if(key.endsWith(SUFFIX_LOGINVALIDSTATEMENTS)) {
				//do nothing here
			}
			else {
				log.warn("unknown prop key format: '"+key+"'");
			}
		}
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("...end processing, total time = "+totalTime+"ms");
	}
	/*static String getExecId(String key, String prefix, String suffix) {
		return key.substring(prefix.length(), key.length()-suffix.length());
	}*/

	static String getExecId(String key, String prefix) {
		int preflen = prefix.length();
		int end = key.indexOf('.', preflen);
		//log.debug("k: "+key+", p: "+preflen+", e: "+end);
		if(end==-1) { return key.substring(preflen); }
		else { return key.substring(preflen, end); }
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		SQLRegen sqlr = new SQLRegen();
		
		try {
			sqlr.init(args);
			sqlr.conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, sqlr.papp);
			sqlr.doIt();
		}
		finally {
			sqlr.end();
		}
	}

}
