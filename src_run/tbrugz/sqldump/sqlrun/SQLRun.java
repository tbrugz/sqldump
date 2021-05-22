package tbrugz.sqldump.sqlrun;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.dbmodel.Column.ColTypeUtil;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.def.CommitStrategy;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Executor;
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.def.Util;
import tbrugz.sqldump.sqlrun.jmx.SQLR;
import tbrugz.sqldump.sqlrun.util.SSLUtil;
import tbrugz.sqldump.util.CLIProcessor;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.JMXUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.ShutdownManager;
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
 * XXX: Fixed Column importer? FC importer with spec...
 * XXXdone: Regex importer (parses log lines, ...)
 * TODOne: add 'global' props: sqlrun.dir / sqlrun.loginvalidstatments
 * TODO: prop 'sqlrun.runonly(inorder)=<id1>, <id2>' - should precede standard 'auto-ids'
 * 
 * see also:
 * http://www.postgresql.org/docs/9.2/static/populate.html
*/
public class SQLRun implements tbrugz.sqldump.def.Executor {
	
	static final Log log = LogFactory.getLog(SQLRun.class);
	static final String PROPERTIES_FILENAME = "sqlrun.properties";
	static final String CONN_PROPS_PREFIX = Constants.SQLRUN_PROPS_PREFIX;
	static final String PRODUCT_NAME = "sqlrun";
	
	//exec properties
	static final String PROP_FILE = "file";
	static final String PROP_FILES = "files";
	static final String PROP_STATEMENT = "statement";
	static final String PROP_QUERY = "query";

	//exec suffixes
	static final String SUFFIX_FILE = "." + PROP_FILE;
	static final String SUFFIX_FILES = "." + PROP_FILES;
	static final String SUFFIX_STATEMENT = "." + PROP_STATEMENT;
	static final String SUFFIX_QUERY = "." + PROP_QUERY;

	//aux suffixes
	static final String SUFFIX_DIR = ".dir";
	static final String SUFFIX_LOGINVALIDSTATEMENTS = ".loginvalidstatments";
	static final String SUFFIX_SPLIT = ".split"; //by semicolon - ';'
	static final String SUFFIX_PARAM = ".param";

	//assert suffixes
	static final String SUFFIX_ASSERT_SQL = ".sql";
	static final String SUFFIX_ASSERT_SQLFILE = ".sqlfile";
	
	//assert aux suffixes
	static final String SUFFIX_ASSERT_ROW_COUNT = ".row-count";
	static final String SUFFIX_ASSERT_ROW_COUNT_EQ = SUFFIX_ASSERT_ROW_COUNT+".eq";
	static final String SUFFIX_ASSERT_ROW_COUNT_GT = SUFFIX_ASSERT_ROW_COUNT+".gt";
	static final String SUFFIX_ASSERT_ROW_COUNT_LT = SUFFIX_ASSERT_ROW_COUNT+".lt";

	//properties
	static final String PROP_COMMIT_STATEGY = Constants.SQLRUN_PROPS_PREFIX+".commit.strategy";
	static final String PROP_LOGINVALIDSTATEMENTS = Constants.SQLRUN_PROPS_PREFIX+SUFFIX_LOGINVALIDSTATEMENTS;
	static final String PROP_FILTERBYIDS = Constants.SQLRUN_PROPS_PREFIX+".filterbyids";
	static final String PROP_FAILONERROR = Constants.SQLRUN_PROPS_PREFIX+".failonerror";
	static final String PROP_CONNPROPPREFIX = Constants.SQLRUN_PROPS_PREFIX+".connpropprefix";
	static final String PROP_TRUST_ALL_CERTS = Constants.SQLRUN_PROPS_PREFIX+".trust-all-certs";
	static final String PROP_JMX_CREATE_MBEAN = Constants.SQLRUN_PROPS_PREFIX+".jmx.create-mbean";

	//suffix groups
	static final String[] PROC_SUFFIXES = { SUFFIX_FILE, SUFFIX_FILES, SUFFIX_STATEMENT, Constants.SUFFIX_IMPORT, SUFFIX_QUERY };
	static final String[] ASSERT_SUFFIXES = { SUFFIX_ASSERT_SQL, SUFFIX_ASSERT_SQLFILE };
	static final String[] AUX_SUFFIXES = { SUFFIX_DIR, SUFFIX_LOGINVALIDSTATEMENTS, SUFFIX_SPLIT, SUFFIX_PARAM, Constants.SUFFIX_BATCH_MODE, Constants.SUFFIX_BATCH_SIZE };
	List<String> allAuxSuffixes = new ArrayList<String>();
	
	//other/reserved props
	static final String PROP_PROCID = "_procid";
	
	public enum ProcType {
		EXEC, ASSERT;
	}
	
	final Properties papp = new ParametrizedProperties();
	Connection conn;
	CommitStrategy commitStrategy = CommitStrategy.FILE;
	List<String> filterByIds = null;
	boolean failonerror = true;
	String defaultEncoding;
	boolean jmxCreateMBean = false;
	
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
		List<String> assertkeys = Utils.getKeysStartingWith(papp, Constants.PREFIX_ASSERT);
		Collections.sort(execkeys);
		Collections.sort(assertkeys);
		log.info("init processing...");
		//Utils.showSysProperties();
		Utils.logEnvironment();

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
		Set<String> assertIds = new TreeSet<String>();
		for(String key: assertkeys) {
			String assertId = getExecId(key, Constants.PREFIX_ASSERT);
			if(procIds.contains(assertId)) {
				throw new ProcessingException("id '"+assertId+"' cannot be both .exec & .assert id");
			}
			if(filterByIds==null || filterByIds.contains(assertId)) {
				assertIds.add(assertId);
			}
		}
		
		Map<String, ProcType> allIds = new TreeMap<String, ProcType>();
		for(String s: procIds) {
			allIds.put(s, ProcType.EXEC);
		}
		for(String s: assertIds) {
			allIds.put(s, ProcType.ASSERT);
		}
		
		//Collections.sort(procIds);
		if(allIds.size()==0) {
			log.warn("no processing/assert ids selected for execution...");
		}
		else {
			log.info("processing ids in order: "+Utils.join(allIds.keySet(), ", ")+" ["+allIds.size()+" ids selected]");
			Thread shutdownThread = getShutdownThread();
			boolean shutdownHookAdded = ShutdownManager.instance().addShutdownHook(shutdownThread);

			doRunIds(allIds);

			if(shutdownHookAdded) {
				ShutdownManager.instance().removeShutdownHook(shutdownThread);
			}
		}
	}

	Thread getShutdownThread() {
		return new Thread() {
			public void run() {
				try {
					if(conn!=null && !conn.isClosed()) {
						boolean isAutocommit = conn.getAutoCommit();
						log.warn("[shutdown] Shutdown detected" + (!isAutocommit ? ". Will rollback":"") );
						if(!isAutocommit) {
							conn.rollback();
						}
						end(true);
					}
					else {
						log.warn("[shutdown] Shutdown detected");
					}
				} catch (SQLException e) {
					log.warn("[shutdown] Error rolling back: "+e, e);
				}
				log.info("[shutdown] Shutting down");
			}
		};
	}
	
	void doRunIds(Map<String, ProcType> allIds) throws IOException, SQLException {
		long initTime = System.currentTimeMillis();
		papp.setProperty(Defs.PROP_START_TIME_MILLIS, String.valueOf(initTime));
		
		if(jmxCreateMBean) {
			sqlrmbean = new SQLR(allIds.size(), conn.getMetaData());
			JMXUtil.registerMBeanSimple(SQLR.MBEAN_NAME, sqlrmbean);
		}
		
		srproc = new StmtProc();
		srproc.setConnection(conn);
		srproc.setDefaultFileEncoding(defaultEncoding);
		srproc.setCommitStrategy(commitStrategy);
		srproc.setProperties(papp);
		srproc.setFailOnError(failonerror);
		
		int sqlrunCounter = 0;
		
		//TODOne: use procIds instead of execkeys (?)
		for(Map.Entry<String, ProcType> e: allIds.entrySet()) {
			String procId = e.getKey();
			//log.info("doRunIds: id="+procId);
			if(e.getValue().equals(ProcType.EXEC)) {
				boolean exec = doExec(procId, sqlrunCounter);
				if(exec) {
					sqlrunCounter++;
				}
			}
			else if(e.getValue().equals(ProcType.ASSERT)) {
				boolean exec = doAssert(procId, sqlrunCounter);
				if(exec) {
					sqlrunCounter++;
				}
			}
			else {
				log.warn("unknown ProcType: "+e.getValue()+" [key="+procId+"]");
			}
		}
		if(commitStrategy==CommitStrategy.RUN) { doCommit(); }
		long totalTime = System.currentTimeMillis() - initTime;
		log.info("...end processing ["+sqlrunCounter+" ids ran], total time = "+totalTime+"ms");
	}
	
	boolean doExec(String procId, int sqlrunCounter) throws IOException, SQLException {
			boolean isExecId = false;
			//String procId = getExecId(key, Constants.PREFIX_EXEC);
			String key = getKeyEndsWithAny(papp, Constants.PREFIX_EXEC+procId, PROC_SUFFIXES);
			//log.info("procid: "+procId);
			if(key==null) {
				//log.warn("no action: "+procId);
				return false;
			}
			//log.info("procid: "+procId+" key: "+key);
			String action = key.substring((Constants.PREFIX_EXEC+procId).length());
			if(filterByIds!=null && !filterByIds.contains(procId)) { return false; }
			
			String execValue = papp.getProperty(key);
			boolean execFailOnError = Utils.getPropBool(papp, Constants.PREFIX_EXEC+procId+Constants.SUFFIX_FAILONERROR, failonerror);
			
			if(endsWithAny(key, PROC_SUFFIXES)) {
				log.info(">>> processing: id = '"+procId+"' ; action = '"+action+"' ; "+
						(key.endsWith(Constants.SUFFIX_IMPORT)?"importer = '"+execValue+"' ; ":"")+
						"failonerror = "+execFailOnError);
				isExecId = true;
				sqlrunCounter++;
				if(sqlrmbean!=null) {
					sqlrmbean.newTaskUpdate(sqlrunCounter, procId, action, execValue);
				}
			}
			else {
				log.warn("no exec suffix for key '"+key+"' [id="+procId+",action="+action+"]");
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
					log.info(fileCount + " files processed");
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
				@SuppressWarnings("unused")
				int urows = srproc.execStatement(execValue);
			}
			// .import
			else if(key.endsWith(Constants.SUFFIX_IMPORT)) {
				String importType = execValue;
				Importer importer = getImporter(importType);
				if(importer==null) {
					log.warn("unknown import type: "+importType);
					return false;
				}
				
				importer.setExecId(procId);
				importer.setConnection(conn);
				importer.setDefaultFileEncoding(defaultEncoding);
				importer.setCommitStrategy(commitStrategy);
				setExecProperties(importer, papp, execFailOnError);
				@SuppressWarnings("unused")
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
	
	boolean doAssert(String procId, int sqlrunCounter) throws IOException, SQLException {
		boolean isAssertId = false;
		String prefix = Constants.PREFIX_ASSERT+procId;
		String key = getKeyEndsWithAny(papp, prefix, ASSERT_SUFFIXES);
		
		//log.info("procid: "+procId+" / "+prefix);
		if(key==null) {
			//log.warn("no action: "+procId);
			return false;
		}
		//log.info("procid: "+procId+" key: "+key);
		String action = key.substring(prefix.length());
		if(filterByIds!=null && !filterByIds.contains(procId)) { return false; }
		
		String execValue = papp.getProperty(key);
		boolean execFailOnError = Utils.getPropBool(papp, prefix + Constants.SUFFIX_FAILONERROR, failonerror);
		
		if(endsWithAny(key, ASSERT_SUFFIXES)) {
			log.info(">>> processing: id = '"+procId+"' ; action = '"+action+"' ; failonerror = "+execFailOnError);
			isAssertId = true;
			sqlrunCounter++;
			if(sqlrmbean!=null) {
				sqlrmbean.newTaskUpdate(sqlrunCounter, procId, action, execValue);
			}
		}
		else {
			log.warn("no assert suffix for key '"+key+"' [id="+procId+",action="+action+"]");
			return false;
		}
		papp.setProperty(PROP_PROCID, procId);
		
		String sql = null;
		
		// .sql
		if(key.endsWith(SUFFIX_ASSERT_SQL)) {
			sql = papp.getProperty(key);
		}
		// .sqlfile
		else if(key.endsWith(SUFFIX_ASSERT_SQLFILE)) {
			String sqlFile = papp.getProperty(key);
			sql = IOUtil.readFromReader(new FileReader(sqlFile));
		}
		else {
			throw new ProcessingException("unknown action: "+action+" [key="+key+"]");
		}
		
		int assertsChecked = 0;
		
		Integer assertRowCountEquals = Utils.getPropInt(papp, prefix + SUFFIX_ASSERT_ROW_COUNT_EQ);
		Integer assertRowCountGreaterThan = Utils.getPropInt(papp, prefix + SUFFIX_ASSERT_ROW_COUNT_GT);
		Integer assertRowCountLessThan = Utils.getPropInt(papp, prefix + SUFFIX_ASSERT_ROW_COUNT_LT);

		PreparedStatement stmt = conn.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		List<String> colNames = DataDumpUtils.getColumnNames(rs.getMetaData());
		int countCols = colNames.size();
		int countRows = 0;
		while(rs.next()) {
			countRows++;
			for(int i=0;i<countCols;i++) {
				String colName = colNames.get(i);
				String prefixRowCol = prefix + ".row@" + countRows + ".col@" + colName;
				String valueEquals = papp.getProperty(prefixRowCol + ".eq");
				if(valueEquals!=null) {
					log.debug("checking for '"+prefixRowCol + ".eq"+"' assert prop");
					String valueFromQuery = rs.getString(colName);
					if(!valueEquals.equals(valueFromQuery)) {
						String message = "[assert #"+procId+"] value '" + valueEquals + "' expected [row="+countRows+";col="+colName+"] but value '"+valueFromQuery+"' found";
						reportAssertError(message);
					}
					assertsChecked++;
				}
			}
		}
		
		if(assertRowCountEquals!=null) {
			if( countRows!=assertRowCountEquals ) {
				String message = "[assert #"+procId+"] " + assertRowCountEquals + " rows expected but "+countRows+" rows found";
				reportAssertError(message);
			}
			assertsChecked++;
		}
		if(assertRowCountGreaterThan!=null) {
			if(! (countRows > assertRowCountGreaterThan) ) {
				String message = "[assert #"+procId+"] more than " + assertRowCountGreaterThan + " rows expected but "+countRows+" rows found";
				reportAssertError(message);
			}
			assertsChecked++;
		}
		if(assertRowCountLessThan!=null) {
			if(! (countRows < assertRowCountLessThan) ) {
				String message = "[assert #"+procId+"] less than " + assertRowCountLessThan + " rows expected but "+countRows+" rows found";
				reportAssertError(message);
			}
			assertsChecked++;
		}
		
		if(assertsChecked==0) {
			String message = "no assert property defined?";
			log.warn(message);
			if(failonerror) {
				throw new ProcessingException(message);
			}
		}
		else {
			log.info("assert: "+assertsChecked+" assertions checked [#rows = "+countRows+" ; #cols = "+countCols+"]");
		}
		return isAssertId;
	}
	
	void reportAssertError(String message) {
		log.warn(message);
		//XXX test for failonerror?
		//XXX throw something like AssertException?
		throw new ProcessingException(message);
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

	static String getKeyEndsWithAny(Properties prop, String key, String[] suffixes) {
		for(String suf: suffixes) {
			String fullKey = key+suf;
			String val = prop.getProperty(fullKey);
			if(val!=null) { return fullKey; }
		}
		return null;
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
	
	void init(Connection c) throws IOException, ClassNotFoundException, SQLException, NamingException {
		ColTypeUtil.setProperties(papp);
		
		allAuxSuffixes.addAll(Arrays.asList(AUX_SUFFIXES));
		List<Executor> loe = getAllExecutors();
		for(Executor e: loe) {
			allAuxSuffixes.addAll(e.getAuxSuffixes());
		}
		filterByIds = Utils.getStringListFromProp(papp, PROP_FILTERBYIDS, ",");
		
		commitStrategy = getCommitStrategy( papp.getProperty(PROP_COMMIT_STATEGY), commitStrategy );
		boolean commitStrategyIsAutocommit = commitStrategy==CommitStrategy.AUTO_COMMIT;
		if(c!=null) {
			conn = c;
			boolean isAutocommit = c.getAutoCommit();
			if(isAutocommit != commitStrategyIsAutocommit) {
				c.setAutoCommit(commitStrategyIsAutocommit);
			}
		}
		else {
			String connPrefix = papp.getProperty(PROP_CONNPROPPREFIX);
			if(connPrefix==null) {
				connPrefix = CONN_PROPS_PREFIX;
			}
			// if connPrefix+".autocommit" is set, show warning
			{
				String autocommitPropKey = connPrefix+ConnectionUtil.SUFFIX_AUTOCOMMIT;
				String autocommitPropValue = papp.getProperty(autocommitPropKey);
				if(autocommitPropValue != null) {
					log.warn("prop '"+autocommitPropKey+"' (value: "+autocommitPropValue+") will be ignored. Commit Strategy is "+commitStrategy);
				}
			}
			conn = ConnectionUtil.initDBConnection(connPrefix, papp, commitStrategyIsAutocommit);
			if(conn==null) {
				throw new ProcessingException("null connection [prop prefix: '"+connPrefix+"']");
			}
		}

		ConnectionUtil.showDBInfo(conn.getMetaData());

		//inits DBMSResources
		DBMSResources.instance().setup(papp);
		//DBMSResources.instance().updateMetaData(conn.getMetaData()); //XXX: really needed?
		
		//inits specific DBMSFeatures class
		//XXX: really needed?
		//DBMSFeatures feats = DBMSResources.instance().databaseSpecificFeaturesClass();
		//log.debug("DBMSFeatures: "+feats);
		
		failonerror = Utils.getPropBool(papp, PROP_FAILONERROR, failonerror);
		defaultEncoding = papp.getProperty(Constants.SQLRUN_PROPS_PREFIX+Constants.SUFFIX_DEFAULT_ENCODING, DataDumpUtils.CHARSET_UTF8);
		jmxCreateMBean = Utils.getPropBool(papp, PROP_JMX_CREATE_MBEAN, jmxCreateMBean);
		
		if(Utils.getPropBool(papp, PROP_TRUST_ALL_CERTS, false)) {
			SSLUtil.trustAll();
		}
	}
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		SQLRun sqlr = new SQLRun();
		sqlr.doMain(args, null, null);
	}
	
	@Override
	public void doMain(String[] args, Properties p) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		doMain(args, p, null);
	}
	
	public void doMain(String[] args, Properties p, Connection c) throws ClassNotFoundException, IOException, SQLException, NamingException, IllegalStateException {
		if(CLIProcessor.shouldStopExec(PRODUCT_NAME, args)) {
			return;
		}
		try {
			if(p!=null) {
				papp.putAll(p);
				if(args!=null) {
					String message = "args informed "+Arrays.asList(args)+" but won't be processed";
					log.warn(message);
					if(failonerror) { //always true?
						throw new IllegalStateException(message);
					}
				}
			}
			else {
				CLIProcessor.init(PRODUCT_NAME, args, PROPERTIES_FILENAME, papp);
			}
			init(c);
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

	static final String[] EXECUTOR_CLASSES = {
		"CSVImporter", "CSVImporterPlain", "FFCImporter", "RegexImporter",
		"StmtProc", "QueryDumper"
	};
	
	static final String[] IMPORTER_IDS = {
		"csv", "csvplain", "ffc", "regex", "xls", "sql"
	};

	static final String[] IMPORTER_CLASSES = {
		"CSVImporter", "CSVImporterPlain", "FFCImporter", "RegexImporter", "XlsImporter", "SqlImporter"
	};
	
	static Importer getImporter(String id) {
		for(int i=0;i<IMPORTER_IDS.length;i++) {
			if(IMPORTER_IDS[i].equals(id)) {
				return (Importer) Utils.getClassInstance(IMPORTER_CLASSES[i], "tbrugz.sqldump.sqlrun.importers");
			}
		}
		return null;
	}

	static List<Executor> getAllExecutors() {
		List<Executor> loe = new ArrayList<Executor>();
		for(int i=0;i<EXECUTOR_CLASSES.length;i++) {
			loe.add( (Executor)Utils.getClassInstance(EXECUTOR_CLASSES[i], "tbrugz.sqldump.sqlrun", "tbrugz.sqldump.sqlrun.importers") );
		}
		return loe;
	}
	
}
