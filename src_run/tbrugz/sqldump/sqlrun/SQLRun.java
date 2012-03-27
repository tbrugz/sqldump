package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: cli (sqlplus-like)? continue/exit on error?
 * XXXxx: show/log ordered 'exec-ids' before executing
 * XXX: option for numeric/alphanumeric proc-ids
 * TODO: one statement per file option
 * XXX: statements 'split-by' option
 * TODO: commit/autocommit? when? each X statements? config per processing?
 * XXX: CSV importer? Fixed Column importer? FC importer with spec...
 * TODOne: add 'global' props: sqlrun.dir / sqlrun.loginvalidstatments
*/
public class SQLRun {
	
	static Log log = LogFactory.getLog(SQLRun.class);
	
	static final String PROPERTIES_FILENAME = "sqlrun.properties";
	static final String SQLRUN_PROPS_PREFIX = "sqlrun"; 
	static final String CONN_PROPS_PREFIX = SQLRUN_PROPS_PREFIX; 
	
	//prefixes
	static final String PREFIX_EXEC = "sqlrun.exec.";

	//suffixes
	static String SUFFIX_FILE = ".file";
	static String SUFFIX_FILES = ".files";
	static String SUFFIX_STATEMENT = ".statement";

	//aux suffixes
	static String SUFFIX_DIR = ".dir";
	static String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	
	//properties
	static String PROP_LOGINVALIDSTATEMENTS = SQLRUN_PROPS_PREFIX+SUFFIX_LOGINVALIDSTATEMENTS;

	//suffix groups
	static String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_FILES, SUFFIX_STATEMENT };
	static String[] AUX_SUFFIXES = { SUFFIX_DIR, SUFFIX_LOGINVALIDSTATEMENTS };
	
	Properties papp = new ParametrizedProperties();
	Connection conn;
	
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
			if(endsWithAny(key, PROC_SUFFIXES)) {
				log.info(">>> processing: id = '"+procId+"'");
			}
			
			// .file
			if(key.endsWith(SUFFIX_FILE)) {
				try {
					srproc.execFileFromKey(key, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS);
				}
				catch(FileNotFoundException e) {
					log.warn("file not found: "+e);
					log.debug("file not found", e);
				}
			}
			// .files
			else if(key.endsWith(SUFFIX_FILES)) {
				try {
					String dir = getDir(procId);
					if(dir==null) {
						log.warn("no '.dir' property...");
						continue;
					}
					File fdir = new File(dir);
					String[] files = fdir.list();
					String fileRegex = papp.getProperty(key);
					for(String file: files) {
						if(file.matches(fileRegex)) {
							srproc.execFile(fdir.getAbsolutePath()+File.separator+file, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS);
						}
					}
				}
				catch(FileNotFoundException e) {
					log.warn("file not found: "+e);
					log.debug("file not found", e);
				}
			}
			// .statement
			else if(key.endsWith(SUFFIX_STATEMENT)) {
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
			else if(endsWithAny(key, AUX_SUFFIXES)) {
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

	String getDir(String procId) {
		String dir = papp.getProperty(PREFIX_EXEC+procId+SUFFIX_DIR);
		if(dir!=null) { return dir; }
		dir = papp.getProperty(SQLRUN_PROPS_PREFIX+SUFFIX_DIR);
		return dir;
	}
	
	static boolean endsWithAny(String key, String[] suffixes) {
		for(String suf: suffixes) {
			if(key.endsWith(suf)) { return true; }
		}
		return false;
	}

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
		SQLRun sqlr = new SQLRun();
		
		try {
			SQLDump.init(args, sqlr.papp);
			sqlr.conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, sqlr.papp);
			if(sqlr.conn==null) { return; }
			sqlr.doIt();
		}
		finally {
			sqlr.end();
		}
	}

}
