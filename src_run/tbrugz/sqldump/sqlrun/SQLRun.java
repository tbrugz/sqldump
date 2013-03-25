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
import tbrugz.sqldump.def.DBMSFeatures;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.sqlrun.importers.AbstractImporter;
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
 * ~TODO: commit/autocommit? when? each X statements? config per processing? (autocommit|statement|file|execid|run)?
 * XXX: each procid may have its own commit strategy?
 * XXX: rollback strategy? savepoint(s)?
 * XXXdone: CSV importer
 * XXX: Fixed Column importer? FC importer with spec...
 * XXXdone: Regex importer (parses log lines, ...)
 * XXX: remove dependency from CSVImporter, RegexImporter
 * TODOne: add 'global' props: sqlrun.dir / sqlrun.loginvalidstatments
 * TODO: prop 'sqlrun.runonly(inorder)=<id1>, <id2>' - should precede standard 'auto-ids'
*/
public class SQLRun {
	
	public static enum CommitStrategy {
		AUTO_COMMIT,
		//STATEMENT, //not implemented yet
		FILE,
		EXEC_ID,
		RUN,
		NONE
	} 
	
	static final Log log = LogFactory.getLog(SQLRun.class);
	static final Log logCommit = LogFactory.getLog(SQLRun.class.getName()+"-commit");
	public static final Log logBatch = LogFactory.getLog(SQLRun.class.getName()+"-batch");

	public static final String STDIN = "<stdin>"; 
	
	static final String PROPERTIES_FILENAME = "sqlrun.properties";
	static final String SQLRUN_PROPS_PREFIX = "sqlrun"; 
	static final String CONN_PROPS_PREFIX = SQLRUN_PROPS_PREFIX; 
	
	//prefixes
	public static final String PREFIX_EXEC = SQLRUN_PROPS_PREFIX + ".exec.";

	//exec suffixes
	static final String SUFFIX_FILE = ".file";
	static final String SUFFIX_FILES = ".files";
	static final String SUFFIX_STATEMENT = ".statement";
	public static final String SUFFIX_IMPORT = ".import";
	static final String SUFFIX_QUERY = ".query";

	//aux suffixes
	static final String SUFFIX_DIR = ".dir";
	static final String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	static final String SUFFIX_SPLIT = ".split"; //by semicolon - ';'
	static final String SUFFIX_PARAM = ".param";
	public static final String SUFFIX_BATCH_MODE = ".batchmode";
	public static final String SUFFIX_BATCH_SIZE = ".batchsize";
	
	//properties
	static final String PROP_COMMIT_STATEGY = "sqlrun.commit.strategy";
	static final String PROP_LOGINVALIDSTATEMENTS = SQLRUN_PROPS_PREFIX+SUFFIX_LOGINVALIDSTATEMENTS;
	static final String PROP_FILTERBYIDS = "sqlrun.filterbyids";
	static final String PROP_SQLTOKENIZERCLASS = "sqlrun.sqltokenizerclass";

	//suffix groups
	static final String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_FILES, SUFFIX_STATEMENT, SUFFIX_IMPORT, SUFFIX_QUERY };
	static final String[] AUX_SUFFIXES = { SUFFIX_DIR, SUFFIX_LOGINVALIDSTATEMENTS, SUFFIX_SPLIT, SUFFIX_PARAM, SUFFIX_BATCH_MODE, SUFFIX_BATCH_SIZE };
	List<String> allAuxSuffixes = new ArrayList<String>();
	
	//other/reserved props
	static final String PROP_PROCID = "_procid";
	
	final Properties papp = new ParametrizedProperties();
	Connection conn;
	CommitStrategy commitStrategy = CommitStrategy.FILE;
	List<String> filterByIds = null;
	
	void end(boolean closeConnection) throws SQLException {
		if(closeConnection && conn!=null) {
			try {
				log.info("closing connection: "+conn);
				//conn.rollback();
				conn.close();
			}
			catch(Exception e) {
				log.warn("exception in close(): "+e); 
			}
		}
		log.info("...done");
	}
	
	void doIt() throws IOException, SQLException {
		List<String> execkeys = Utils.getKeysStartingWith(papp, PREFIX_EXEC);
		Collections.sort(execkeys);
		log.info("init processing...");
		//Utils.showSysProperties();
		Utils.logEnvironment();
		long initTime = System.currentTimeMillis();

		Set<String> procIds = new TreeSet<String>();
		if(filterByIds!=null) {
			log.info("filter by ids: "+filterByIds);
		}
		for(String key: execkeys) {
			String procId = getExecId(key, PREFIX_EXEC);
			if(filterByIds==null || filterByIds.contains(procId)) {
				procIds.add(procId);
			}
		}
		//Collections.sort(procIds);
		if(procIds.size()==0) {
			log.warn("no processing ids selected for execution...");
		}
		else {
			log.info("processing ids in exec order: "+Utils.join(procIds, ", ")+" ["+procIds.size()+" ids selected]");
		}
		
		StmtProc srproc = new StmtProc();
		srproc.setConnection(conn);
		srproc.setCommitStrategy(commitStrategy);
		srproc.setProperties(papp);
		srproc.setFailOnError(true); //XXX setFailOnError by properties...
		//TODO: use procIds instead of execkeys (?)
		for(String key: execkeys) {
			boolean isExecId = false;
			String procId = getExecId(key, PREFIX_EXEC);
			String action = key.substring((PREFIX_EXEC+procId).length());
			if(filterByIds!=null && !filterByIds.contains(procId)) { continue; }
			
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
					int fileCount = 0;
					for(String file: files) {
						if((fileCount>0) && (commitStrategy==CommitStrategy.FILE)) { doCommit(); }
						srproc.execFile(file, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
						fileCount++;
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
				int urows = srproc.execStatement(stmtStr);
			}
			// .import
			else if(key.endsWith(SUFFIX_IMPORT)) {
				String importType = papp.getProperty(key);
				AbstractImporter importer = null;
				//csv
				if("CSV".equalsIgnoreCase(importType)) {
					importer = new CSVImporter();
				}
				//regex
				else if("REGEX".equalsIgnoreCase(importType)) {
					importer = new RegexImporter();
				}
				else {
					log.warn("unknown import type: "+importType);
					continue;
				}
				
				importer.setExecId(procId);
				importer.setConnection(conn);
				importer.setCommitStrategy(commitStrategy);
				importer.setProperties(papp);
				long imported = 0;
				try {
					imported = importer.importData();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			// .query
			else if(key.endsWith(SUFFIX_QUERY)) {
				QueryDumper sqd = new QueryDumper();
				sqd.setExecId(procId);
				sqd.setConnection(conn);
				sqd.setProperties(papp);
				sqd.execQuery(papp.getProperty(key));
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
				if(commitStrategy==CommitStrategy.EXEC_ID || commitStrategy==CommitStrategy.FILE) {
					doCommit();
				}
				Utils.logMemoryUsage();
			}
		}
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("...end processing, total time = "+totalTime+"ms");
		
		if(commitStrategy==CommitStrategy.RUN) { doCommit(); }
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
		doCommit(conn);
	}
	
	public static void doCommit(Connection conn) {
		try {
			//log.debug("committing...");
			conn.commit();
			logCommit.debug("committed!");
		} catch (SQLException e) {
			logCommit.warn("error commiting: "+e);
		}
	}

	static String getExecId(String key, String prefix) {
		int preflen = prefix.length();
		int end = key.indexOf('.', preflen);
		//log.debug("k: "+key+", p: "+preflen+", e: "+end);
		if(end==-1) { return key.substring(preflen); }
		else { return key.substring(preflen, end); }
	}
	
	static CommitStrategy getCommitStrategy(String commit, CommitStrategy defaultStrategy) {
		CommitStrategy cs = defaultStrategy;
		if(commit==null) {}
		else if(commit.equals("autocommit")) { cs = CommitStrategy.AUTO_COMMIT; }
		else if(commit.equals("file")) { cs = CommitStrategy.FILE; }
		else if(commit.equals("execid")) { cs = CommitStrategy.EXEC_ID; }
		else if(commit.equals("run")) { cs = CommitStrategy.RUN; }
		else if(commit.equals("none")) { cs = CommitStrategy.NONE; }
		else {
			log.warn("unknown commit strategy: "+commit);
		}
		log.info("setting "+(commit==null?"default ":"")+"commit strategy ["+cs+"]");
		return cs;
	}
	
	void init(String args[], Connection c) throws IOException, ClassNotFoundException, SQLException, NamingException {
		SQLDump.init(args, papp);
		allAuxSuffixes.addAll(Arrays.asList(AUX_SUFFIXES));
		allAuxSuffixes.addAll(new CSVImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new RegexImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new QueryDumper().getAuxSuffixes());
		filterByIds = Utils.getStringListFromProp(papp, PROP_FILTERBYIDS, ",");
		
		commitStrategy = getCommitStrategy( papp.getProperty(PROP_COMMIT_STATEGY), commitStrategy );
		if(c!=null) {
			conn = c;
		}
		else {
			conn = SQLUtils.ConnectionUtil.initDBConnection(CONN_PROPS_PREFIX, papp, commitStrategy==CommitStrategy.AUTO_COMMIT);
		}
		SQLUtils.ConnectionUtil.showDBInfo(conn.getMetaData());

		//inits DBMSResources
		DBMSResources.instance().setup(papp);
		DBMSResources.instance().updateMetaData(conn.getMetaData());
		
		//inits specific DBMSFeatures class
		//XXX: really needed?
		DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		log.debug("DBMSFeatures: "+feats);
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
		sqlr.doMain(args, null, null);
	}
	
	public void doMain(String[] args, Properties p) throws ClassNotFoundException, IOException, SQLException, NamingException {
		doMain(args, p, null);
	}
	
	public void doMain(String[] args, Properties p, Connection c) throws ClassNotFoundException, IOException, SQLException, NamingException {
		try {
			if(p!=null) { papp.putAll(p); }
			init(args, c);
			if(conn==null) { return; }
			doIt();
		}
		finally {
			end(c==null);
		}
	}

}
