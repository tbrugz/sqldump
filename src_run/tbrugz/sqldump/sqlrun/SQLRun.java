package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.SQLUtils;
import tbrugz.sqldump.sqlrun.importers.CSVImporter;
import tbrugz.sqldump.sqlrun.importers.RegexImporter;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.Utils;

/*
 * TODO: cli (sqlplus-like)? continue/exit on error?
 * XXXxx: show/log ordered 'exec-ids' before executing
 * XXX: option for numeric/alphanumeric proc-ids
 * TODOne: one statement per file option -> split-true|false
 * XXXxx: statements 'split-by' option
 * TODO: commit/autocommit? when? each X statements? config per processing? (autocommit|statement|file|execid|run)?
 * XXX: each procid may have its own commit strategy?
 * XXX: rollback strategy? savepoint(s)?
 * XXX: CSV importer? Fixed Column importer? FC importer with spec...
 * XXX: Regex importer (parses log lines, ...)
 * XXX: remove dependency from CSVImporter
 * TODOne: add 'global' props: sqlrun.dir / sqlrun.loginvalidstatments
 * TODO: prop 'sqlrun.runonly(inorder)=<id1>, <id2>' - should precede standard 'auto-ids'
*/
public class SQLRun {
	
	public static enum CommitStrategy {
		AUTO_COMMIT,
		//STATEMENT, //not implemented yet
		//FILE, //not implemented yet
		EXEC_ID,
		RUN,
		NONE
	} 
	
	static Log log = LogFactory.getLog(SQLRun.class);

	public static final String STDIN = "<stdin>"; 
	
	static final String PROPERTIES_FILENAME = "sqlrun.properties";
	static final String SQLRUN_PROPS_PREFIX = "sqlrun"; 
	static final String CONN_PROPS_PREFIX = SQLRUN_PROPS_PREFIX; 
	
	//prefixes
	public static final String PREFIX_EXEC = SQLRUN_PROPS_PREFIX + ".exec.";

	//exec suffixes
	static String SUFFIX_FILE = ".file";
	static String SUFFIX_FILES = ".files";
	static String SUFFIX_STATEMENT = ".statement";
	static String SUFFIX_IMPORT = ".import";

	//aux suffixes
	static String SUFFIX_DIR = ".dir";
	static String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	static String SUFFIX_SPLIT = ".split"; //by semicolon - ';'
	static String SUFFIX_PARAM = ".param";
	
	//properties
	static final String PROP_COMMIT_STATEGY = "sqlrun.commit.strategy";
	static String PROP_LOGINVALIDSTATEMENTS = SQLRUN_PROPS_PREFIX+SUFFIX_LOGINVALIDSTATEMENTS;

	//suffix groups
	static String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_FILES, SUFFIX_STATEMENT, SUFFIX_IMPORT };
	static String[] AUX_SUFFIXES = { SUFFIX_DIR, SUFFIX_LOGINVALIDSTATEMENTS, SUFFIX_SPLIT, SUFFIX_PARAM };
	List<String> allAuxSuffixes = new ArrayList<String>();
	
	//other/reserved props
	static final String PROP_PROCID = "_procid";
	
	Properties papp = new ParametrizedProperties();
	Connection conn;
	CommitStrategy commitStrategty = CommitStrategy.RUN;
	
	void end() throws SQLException {
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
			boolean isExecId = false;
			String procId = getExecId(key, PREFIX_EXEC);
			String action = key.substring((PREFIX_EXEC+procId).length());
			if(endsWithAny(key, PROC_SUFFIXES)) {
				log.info(">>> processing: id = '"+procId+"' ; action = '"+action+"'");
				isExecId = true;
			}
			papp.setProperty(PROP_PROCID, procId);
			
			boolean splitBySemicolon = Utils.getPropBool(papp, PREFIX_EXEC+procId+SUFFIX_SPLIT, true);
			
			// .file
			if(key.endsWith(SUFFIX_FILE)) {
				try {
					srproc.execFile(papp.getProperty(key), PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
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
					String fileRegex = papp.getProperty(key);
					List<String> files = getFiles(dir, fileRegex);
					for(String file: files) {
						srproc.execFile(file, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
					}
					/*if(dir==null) {
						log.warn("no '.dir' property...");
						continue;
					}
					File fdir = new File(dir);
					String[] files = fdir.list();
					String fileRegex = papp.getProperty(key);
					for(String file: files) {
						if(file.matches(fileRegex)) {
							srproc.execFile(fdir.getAbsolutePath()+File.separator+file, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
						}
					}*/
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
			// .import
			else if(key.endsWith(SUFFIX_IMPORT)) {
				String importType = papp.getProperty(key);
				long imported = 0;
				//csv
				if("CSV".equalsIgnoreCase(importType)) {
					CSVImporter importer = new CSVImporter();
					importer.setExecId(procId);
					importer.setProperties(papp);
					importer.setConnection(conn);
					try {
						imported = importer.importData();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				//regex
				else if("REGEX".equalsIgnoreCase(importType)) {
					RegexImporter importer = new RegexImporter();
					importer.setExecId(procId);
					importer.setProperties(papp);
					importer.setConnection(conn);
					try {
						imported = importer.importData();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				else {
					log.warn("unknown import type: "+importType);
				}
			}
			// ...else
			else if(endsWithAny(key, allAuxSuffixes)) {
				//do nothing here
			}
			else if(startsWithAny(action, allAuxSuffixes)) {
				//do nothing here
			}
			else {
				log.warn("unknown prop key format: '"+key+"'");
			}
			
			if(isExecId) {
				if(commitStrategty==CommitStrategy.EXEC_ID) { doCommit(); }
			}
		}
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("...end processing, total time = "+totalTime+"ms");
		
		if(commitStrategty==CommitStrategy.RUN) { doCommit(); }
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

	static boolean endsWithAny(String key, List<String> suffixes) {
		for(String suf: suffixes) {
			if(key.endsWith(suf)) { return true; }
		}
		return false;
	}

	static boolean startsWithAny(String key, List<String> prefixes) {
		for(String pre: prefixes) {
			if(key.startsWith(pre)) { return true; }
		}
		return false;
	}
	
	void doCommit() throws SQLException {
		log.debug("committing...");
		conn.commit();
	}

	static String getExecId(String key, String prefix) {
		int preflen = prefix.length();
		int end = key.indexOf('.', preflen);
		//log.debug("k: "+key+", p: "+preflen+", e: "+end);
		if(end==-1) { return key.substring(preflen); }
		else { return key.substring(preflen, end); }
	}
	
	static CommitStrategy getCommitStrategy(String commit) {
		CommitStrategy cs = CommitStrategy.RUN;
		if(commit==null) {}
		else if(commit.equals("autocommit")) { cs = CommitStrategy.AUTO_COMMIT; }
		else if(commit.equals("execid")) { cs = CommitStrategy.EXEC_ID; }
		else if(commit.equals("run")) { cs = CommitStrategy.RUN; }
		else if(commit.equals("none")) { cs = CommitStrategy.NONE; }
		else {
			log.warn("unknown commit strategy: "+commit);
		}
		log.info("setting "+(commit==null?"default ":"")+"commit strategy ["+cs+"]");
		return cs;
	}
	
	void init(String args[]) throws IOException, ClassNotFoundException, SQLException, NamingException {
		SQLDump.init(args, papp);
		allAuxSuffixes.addAll(Arrays.asList(AUX_SUFFIXES));
		allAuxSuffixes.addAll(new CSVImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new RegexImporter().getAuxSuffixes());
		
		commitStrategty = getCommitStrategy( papp.getProperty(PROP_COMMIT_STATEGY) );
		conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, papp, commitStrategty==CommitStrategy.AUTO_COMMIT);
		SQLUtils.ConnectionUtil.showDBInfo(conn.getMetaData());
	}
	
	public static List<String> getFiles(String dir, String fileRegex) {
		List<String> ret = new ArrayList<String>();
		if(dir==null) {
			log.warn("dir '"+dir+"' not found...");
			return null;
		}
		File fdir = new File(dir);
		String[] files = fdir.list();
		if(files==null) {
			return null;
		}
		for(String file: files) {
			if(file.matches(fileRegex)) {
				ret.add(fdir.getAbsolutePath()+File.separator+file);
			}
		}
		return ret;
	}
	
	/**
	 * @param args
	 * @throws NamingException 
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws Exception 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, NamingException {
		SQLRun sqlr = new SQLRun();
		
		try {
			sqlr.init(args);
			if(sqlr.conn==null) { return; }
			sqlr.doIt();
		}
		finally {
			sqlr.end();
		}
	}

}
