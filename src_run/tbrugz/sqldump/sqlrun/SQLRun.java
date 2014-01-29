package tbrugz.sqldump.sqlrun;

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

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.dbmd.DBMSFeatures;
import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.def.CommitStrategy;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Executor;
import tbrugz.sqldump.sqlrun.def.Util;
import tbrugz.sqldump.sqlrun.importers.AbstractImporter;
import tbrugz.sqldump.sqlrun.importers.CSVImporter;
import tbrugz.sqldump.sqlrun.importers.FFCImporter;
import tbrugz.sqldump.sqlrun.importers.RegexImporter;
import tbrugz.sqldump.sqlrun.jmx.SQLR;
import tbrugz.sqldump.sqlrun.util.SSLUtil;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.JMXUtil;
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
public class SQLRun implements tbrugz.sqldump.def.Executor {
	
	static final Log log = LogFactory.getLog(SQLRun.class);
	static final String PROPERTIES_FILENAME = "sqlrun.properties";
	static final String CONN_PROPS_PREFIX = Constants.SQLRUN_PROPS_PREFIX; 
	
	//exec suffixes
	static final String SUFFIX_FILE = ".file";
	static final String SUFFIX_FILES = ".files";
	static final String SUFFIX_STATEMENT = ".statement";
	static final String SUFFIX_QUERY = ".query";

	//aux suffixes
	static final String SUFFIX_DIR = ".dir";
	static final String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	static final String SUFFIX_SPLIT = ".split"; //by semicolon - ';'
	static final String SUFFIX_PARAM = ".param";

	//properties
	static final String PROP_COMMIT_STATEGY = Constants.SQLRUN_PROPS_PREFIX+".commit.strategy";
	static final String PROP_LOGINVALIDSTATEMENTS = Constants.SQLRUN_PROPS_PREFIX+SUFFIX_LOGINVALIDSTATEMENTS;
	static final String PROP_FILTERBYIDS = Constants.SQLRUN_PROPS_PREFIX+".filterbyids";
	static final String PROP_FAILONERROR = Constants.SQLRUN_PROPS_PREFIX+".failonerror";
	static final String PROP_CONNPROPPREFIX = Constants.SQLRUN_PROPS_PREFIX+".connpropprefix";
	static final String PROP_TRUST_ALL_CERTS = Constants.SQLRUN_PROPS_PREFIX+".trust-all-certs";

	//suffix groups
	static final String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_FILES, SUFFIX_STATEMENT, Constants.SUFFIX_IMPORT, SUFFIX_QUERY };
	static final String[] AUX_SUFFIXES = { SUFFIX_DIR, SUFFIX_LOGINVALIDSTATEMENTS, SUFFIX_SPLIT, SUFFIX_PARAM, Constants.SUFFIX_BATCH_MODE, Constants.SUFFIX_BATCH_SIZE };
	List<String> allAuxSuffixes = new ArrayList<String>();
	
	//other/reserved props
	static final String PROP_PROCID = "_procid";
	
	final Properties papp = new ParametrizedProperties();
	Connection conn;
	CommitStrategy commitStrategy = CommitStrategy.FILE;
	List<String> filterByIds = null;
	boolean failonerror = true;
	String defaultEncoding;
	
	SQLR sqlrmbean;
	StmtProc srproc;
	
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
		List<String> execkeys = Utils.getKeysStartingWith(papp, Constants.PREFIX_EXEC);
		Collections.sort(execkeys);
		log.info("init processing...");
		//Utils.showSysProperties();
		Utils.logEnvironment();
		long initTime = System.currentTimeMillis();
		papp.setProperty(Defs.PROP_START_TIME_MILLIS, String.valueOf(initTime));

		Set<String> procIds = new TreeSet<String>();
		if(filterByIds!=null) {
			log.info("filter by ids: "+filterByIds);
		}
		for(String key: execkeys) {
			String procId = getExecId(key, Constants.PREFIX_EXEC);
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
		
		sqlrmbean = new SQLR(procIds.size(), conn.getMetaData());
		JMXUtil.registerMBeanSimple(SQLR.MBEAN_NAME, sqlrmbean);
		
		srproc = new StmtProc();
		srproc.setConnection(conn);
		srproc.setDefaultFileEncoding(defaultEncoding);
		srproc.setCommitStrategy(commitStrategy);
		srproc.setProperties(papp);
		srproc.setFailOnError(failonerror);
		
		int sqlrunCounter = 0;
		
		//TODO: use procIds instead of execkeys (?)
		for(String key: execkeys) {
			boolean exec = doExec(key, sqlrunCounter);
			if(exec) {
				sqlrunCounter++;
			}
		}
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("...end processing ["+sqlrunCounter+" ids runned], total time = "+totalTime+"ms");
		
		if(commitStrategy==CommitStrategy.RUN) { doCommit(); }
	}
	
	boolean doExec(String key, int sqlrunCounter) throws IOException, SQLException {
			boolean isExecId = false;
			String procId = getExecId(key, Constants.PREFIX_EXEC);
			String action = key.substring((Constants.PREFIX_EXEC+procId).length());
			if(filterByIds!=null && !filterByIds.contains(procId)) { return false; }
			
			String execValue = papp.getProperty(key);
			boolean execFailOnError = Utils.getPropBool(papp, Constants.PREFIX_EXEC+procId+Constants.SUFFIX_FAILONERROR, failonerror);
			
			if(endsWithAny(key, PROC_SUFFIXES)) {
				log.info(">>> processing: id = '"+procId+"' ; action = '"+action+"' ; failonerror = "+execFailOnError);
				isExecId = true;
				sqlrunCounter++;
				sqlrmbean.newTaskUpdate(sqlrunCounter, procId, action, execValue);
			}
			else {
				return false;
			}
			papp.setProperty(PROP_PROCID, procId);
			
			boolean splitBySemicolon = Utils.getPropBool(papp, Constants.PREFIX_EXEC+procId+SUFFIX_SPLIT, true);
			
			// .file
			if(key.endsWith(SUFFIX_FILE)) {
				try {
					setExecProperties(srproc, papp, execFailOnError);
					srproc.execFile(execValue, Constants.PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
				}
				catch(FileNotFoundException e) {
					log.warn("file not found: "+e);
					log.debug("file not found", e);
					if(failonerror) {
						throw new ProcessingException(e);
					}
				}
			}
			// .files
			else if(key.endsWith(SUFFIX_FILES)) {
				try {
					setExecProperties(srproc, papp, execFailOnError);
					String dir = getDir(procId);
					List<String> files = Util.getFiles(dir, execValue);
					int fileCount = 0;
					if(files!=null && files.size()>0) {
					for(String file: files) {
						if((fileCount>0) && (commitStrategy==CommitStrategy.FILE)) { doCommit(); }
						srproc.execFile(file, Constants.PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS, splitBySemicolon);
						fileCount++;
					}
					}
					else {
						log.warn("no files selected in dir '"+dir+"'");
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
				setExecProperties(srproc, papp, execFailOnError);
				int urows = srproc.execStatement(execValue);
			}
			// .import
			else if(key.endsWith(Constants.SUFFIX_IMPORT)) {
				String importType = execValue;
				AbstractImporter importer = null;
				//csv
				if("CSV".equalsIgnoreCase(importType)) {
					importer = new CSVImporter();
				}
				//regex
				else if("REGEX".equalsIgnoreCase(importType)) {
					importer = new RegexImporter();
				}
				//ffc
				else if("FFC".equalsIgnoreCase(importType)) {
					importer = new FFCImporter();
				}
				else {
					log.warn("unknown import type: "+importType);
					return false;
				}
				
				importer.setExecId(procId);
				importer.setConnection(conn);
				importer.setDefaultFileEncoding(defaultEncoding);
				importer.setCommitStrategy(commitStrategy);
				setExecProperties(importer, papp, execFailOnError);
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
				sqd.setDefaultFileEncoding(defaultEncoding);
				setExecProperties(sqd, papp, execFailOnError);
				sqd.execQuery(execValue);
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
				return true;
			}
			return false;
	}
	
	/*static String getExecId(String key, String prefix, String suffix) {
		return key.substring(prefix.length(), key.length()-suffix.length());
	}*/
	
	void setExecProperties(Executor exec, Properties prop, boolean failonerror) {
		exec.setFailOnError(failonerror);
		exec.setProperties(prop);
	}

	String getDir(String procId) {
		String dir = papp.getProperty(Constants.PREFIX_EXEC+procId+SUFFIX_DIR);
		if(dir!=null) { return dir; }
		dir = papp.getProperty(Constants.SQLRUN_PROPS_PREFIX+SUFFIX_DIR);
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
		Util.doCommit(conn);
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
		CLIProcessor.init("sqlrun", args, PROPERTIES_FILENAME, papp);
		ColTypeUtil.setProperties(papp);
		
		allAuxSuffixes.addAll(Arrays.asList(AUX_SUFFIXES));
		allAuxSuffixes.addAll(new StmtProc().getAuxSuffixes());
		allAuxSuffixes.addAll(new CSVImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new RegexImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new FFCImporter().getAuxSuffixes());
		allAuxSuffixes.addAll(new QueryDumper().getAuxSuffixes());
		filterByIds = Utils.getStringListFromProp(papp, PROP_FILTERBYIDS, ",");
		
		commitStrategy = getCommitStrategy( papp.getProperty(PROP_COMMIT_STATEGY), commitStrategy );
		if(c!=null) {
			conn = c;
		}
		else {
			String connPrefix = papp.getProperty(PROP_CONNPROPPREFIX);
			if(connPrefix==null) {
				connPrefix = CONN_PROPS_PREFIX;
			}
			conn = ConnectionUtil.initDBConnection(connPrefix, papp, commitStrategy==CommitStrategy.AUTO_COMMIT);
			if(conn==null) {
				throw new ProcessingException("null connection [prop prefix: '"+connPrefix+"']");
			}
		}

		ConnectionUtil.showDBInfo(conn.getMetaData());

		//inits DBMSResources
		DBMSResources.instance().setup(papp);
		DBMSResources.instance().updateMetaData(conn.getMetaData()); //XXX: really needed?
		
		//inits specific DBMSFeatures class
		//XXX: really needed?
		DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		log.debug("DBMSFeatures: "+feats);
		
		failonerror = Utils.getPropBool(papp, PROP_FAILONERROR, failonerror);
		defaultEncoding = papp.getProperty(Constants.SQLRUN_PROPS_PREFIX+Constants.SUFFIX_DEFAULT_ENCODING, DataDumpUtils.CHARSET_UTF8);
		
		if(Utils.getPropBool(papp, PROP_TRUST_ALL_CERTS, false)) {
			SSLUtil.trustAll();
		}
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
	
	@Override
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

	@Override
	public void setFailOnError(boolean failonerror) {
		this.failonerror = failonerror;
	}
	
}
