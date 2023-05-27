package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.datadump.DataDumpUtils;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.sqlrun.def.CommitStrategy;
import tbrugz.sqldump.sqlrun.def.Constants;
import tbrugz.sqldump.sqlrun.def.Executor;
import tbrugz.sqldump.sqlrun.def.Util;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerStrategy;
import tbrugz.sqldump.sqlrun.tokenzr.TokenizerUtil;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.MathUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringUtils;
import tbrugz.sqldump.util.Utils;

//XXX: remove references to SQLRun class
public class StmtProc extends AbstractFailable implements Executor {

	static final Log log = LogFactory.getLog(StmtProc.class);
	static final Log logRow = LogFactory.getLog(StmtProc.class.getName()+"-row");
	static final Log logStmt = LogFactory.getLog(StmtProc.class.getName()+"-stmt");
	static final Log logUpdates = LogFactory.getLog(StmtProc.class.getName()+"-updates");
	static final Log logInner = LogFactory.getLog(StmtProc.class.getName()+"-inner");
	
	//properties
	static final String PROP_SQLTOKENIZERCLASS = "sqlrun.sqltokenizerclass";
	static final String PROP_USE_PREPARED_STATEMENT = "sqlrun.usepreparedstatement";
	static final String PROP_USE_SAVEPOINT = "sqlrun.use-savepoint";
	
	//suffixes
	static final String SUFFIX_ESCAPE_BACKSLASHED_APOS = "escapebackslashedapos";
	
	static final boolean DEFAULT_USE_BATCH_UPDATE = false;
	static final boolean DEFAULT_ESCAPE_BACKSLASHED_APOS = false;
	static final long DEFAULT_BATCH_SIZE = 1000L;
	static final long DEFAULT_LOG_EACH_X_INPUT_ROWS = 1000L;

	boolean usePreparedStatement = true;
	boolean replacePropsOnFileContents = true; //XXX: add prop for 'replacePropsOnFileContents'
	boolean rollbackOnError = true; //XXX: add prop for 'rollbackOnError'?
	boolean useSavepoint = true;
	
	boolean useBatchUpdate = DEFAULT_USE_BATCH_UPDATE;
	boolean escapeBackslashedApos = DEFAULT_ESCAPE_BACKSLASHED_APOS;
	long batchSize = DEFAULT_BATCH_SIZE;
	String defaultInputEncoding = DataDumpUtils.CHARSET_UTF8;
	String inputEncoding = defaultInputEncoding;
	long logEachXStmts = DEFAULT_LOG_EACH_X_INPUT_ROWS;
	
	TokenizerStrategy tokenizerStrategy = TokenizerStrategy.DEFAULT_STRATEGY;
	
	Connection conn;
	String execId;
	Properties papp;
	CommitStrategy commitStrategy;
	
	static final String[] EXEC_SUFFIXES = {
		SQLRun.SUFFIX_FILE,
		SQLRun.SUFFIX_FILES,
		SQLRun.SUFFIX_STATEMENT
	};
	
	long batchExecCounter = 0;
	Statement batchStmt = null;

	//@SuppressWarnings("deprecation")
	public void execFile(String filePath, String errorLogKey, boolean split) throws IOException {
		setupProperties();
		//String errorLogFilePath = papp.getProperty(errorLogKey);
		File file = new File(filePath);
		//FIXedME: SQLStmtTokenizer not working (on big files?)
		Iterable<String> stmtTokenizer = TokenizerStrategy.getTokenizer(tokenizerStrategy, file, inputEncoding, escapeBackslashedApos, split);

		Writer logerror = null;
		
		log.info("file exec: statements from file '"+file+"'...");
		long urowsTotal = 0;
		long countOk = 0;
		long countError = 0;
		long countExec = 0;
		//boolean errorFileNotFoundWarned = false;
		long initTime = System.currentTimeMillis();
		
		for(String stmtStr: stmtTokenizer) {
			if(stmtStr==null) {
				log.warn("tokenizer returned null statement? [tokenizer = "+tokenizerStrategy.getClass().getSimpleName()+"]");
				continue;
			}
			stmtStr = stmtStr.trim();
			if(stmtStr.equals("")) {
				//log.debug("tokenizer returned empty statement [tokenizer = "+tokenizerStrategy.getClass().getSimpleName()+"]");
				continue;
			}
			// removing SQL comments before check
			if(!TokenizerUtil.containsSqlStatmement(stmtStr)) {
				//log.info("statement is empty or comments-only [tokenizer = "+tokenizerStrategy.getClass().getSimpleName()+"]");
				//log.debug("empty or comments-only statement: ["+stmtStr+"]");
				log.debug("statement is empty or comments-only [tokenizer = "+tokenizerStrategy.getClass().getSimpleName()+"][stmt = "+stmtStr+"]");
				continue;
			}
			
			try {
				//log.debug("stmt: "+stmtStr);
				if(replacePropsOnFileContents) {
					//replacing ${...} parameters
					stmtStr = ParametrizedProperties.replaceProps(stmtStr, papp);
				}
				urowsTotal += execStatementInternal(stmtStr);
				countOk++;
			}
			catch(SQLException e) {
				if(logerror==null) {
					logerror = getErrorLogHandler(errorLogKey);
				}
				if(logerror!=null) {
					logerror.write(stmtStr+";\n");
				}
				countError++;
				logStmt.warn("error executing updates [#ok = "+countOk+",#error = "+countError+"][stmt = "+stmtStr+"]: "+StringUtils.exceptionTrimmed(e));
				logStmt.debug("error executing updates", e);
				SQLUtils.xtraLogSQLException(e, logInner);
				if(failonerror) { throw new ProcessingException(e); }
			}
			countExec++;
			
			if((countExec>0) && (countExec % logEachXStmts)==0) {
				logRow.info(countExec+" statements processed");
			}
		}
		try {
			urowsTotal += closeStatement();
		} catch (SQLException e) {
			logStmt.warn("error closing statement (batch mode?): "+e);
			logStmt.debug("error closing statement (batch mode?)",e);
		}
		
		long totalTime = System.currentTimeMillis() - initTime;
		//commit?
		double statementsPerSec = Double.NaN;
		double updatesPerSec = Double.NaN;
		try {
			statementsPerSec = ((double) countExec) / ( ((double) totalTime) / 1000 );
			updatesPerSec = ((double) urowsTotal) / ( ((double) totalTime) / 1000 );
		}
		catch(ArithmeticException e) {}
		
		log.info("exec = "+countExec+" [ok = "+countOk+", error = "+countError+"], rows updated = "+urowsTotal
				+", elapsed = "+totalTime+"ms"
				+", stmt/s = "+((int)statementsPerSec)
				+", updates/s = "+((int)updatesPerSec)
				+" [file = '"+file.getAbsolutePath()+"']");
		if(logerror!=null) {
			logerror.close();
			log.warn(""+countError+" erroneous statements logged");
			//log.warn(""+countError+" erroneous statements in '"+errorLogFilePath+"'");
		}
	}
	
	boolean errorFileNotFoundWarned = false;
	
	Writer getErrorLogHandler(String errorLogKey) {
		String errorLogFilePath = papp.getProperty(errorLogKey);
		if(errorLogFilePath==null) {
			errorLogFilePath = papp.getProperty(SQLRun.PROP_LOGINVALIDSTATEMENTS);
		}
		if(errorLogFilePath==null) {
			log.warn("error log file not defined [prop '"+SQLRun.PROP_LOGINVALIDSTATEMENTS+"']");
			errorFileNotFoundWarned = true;
			return null;
		}
		
		FileWriter logerror = null;
		try {
			File f = new File(errorLogFilePath);
			File dir = f.getParentFile();
			if(!dir.isDirectory()) {
				log.debug("creating dir: "+dir);
				dir.mkdirs();
			}
			logerror = new FileWriter(errorLogFilePath, true);
		}
		catch (FileNotFoundException fnfe) {
			if(!errorFileNotFoundWarned) {
				log.warn("error opening file '"+errorLogFilePath+"' for writing invalid statements. Ex: "+fnfe);
				errorFileNotFoundWarned = true;
			}
		}
		catch (IOException e) {
			log.warn("ioexception when opening error log file. Ex: "+e);
			errorFileNotFoundWarned = true;
		}
		return logerror;
	}
	
	public int execStatement(String stmtStr) throws IOException {
		setupProperties();
		try {
			long initTime = System.currentTimeMillis();
			
			int urows = execStatementInternal(stmtStr);
			urows += closeStatement();
			
			long totalTime = System.currentTimeMillis() - initTime;
			log.info("statement exec: updates = "+urows+" [elapsed = "+totalTime+"ms]");
			log.debug("statement: "+stmtStr);
			return urows;
		}
		catch(SQLException e) {
			log.warn("error executing statement [stmt = "+stmtStr+"]: "+e);
			log.debug("error executing statement", e);
			SQLUtils.xtraLogSQLException(e, logInner);
			if(failonerror) { throw new ProcessingException(e); }
			return 0;
		}
	}
	
	int execStatementInternal(String stmtStr) throws IOException, SQLException {
		if(stmtStr==null) { throw new IllegalArgumentException("null parameter"); }
		stmtStr = stmtStr.trim();
		if(stmtStr.equals("")) { throw new IllegalArgumentException("null parameter"); }
		
		Savepoint sp = null;
		if(rollbackOnError && useSavepoint && !commitStrategy.equals(CommitStrategy.AUTO_COMMIT)) {
			sp = ConnectionUtil.setSavepoint(conn);
		}
		
		logStmt.debug("executing sql: "+stmtStr);
		try {
			int updateCount = 0;
			if(useBatchUpdate) {
				if(batchStmt==null) {
					batchStmt = conn.createStatement();
				}
				batchStmt.addBatch(replaceParameters(stmtStr));
				batchExecCounter++;
				
				if((batchExecCounter%batchSize)==0) {
					int[] updateCounts = batchStmt.executeBatch();
					//if(log.isInfoEnabled()) { SQLUtils.logWarningsInfo(batchStmt.getWarnings(), log); }
					updateCount = MathUtil.sumInts(updateCounts);
					logStmt.debug("executeBatch(): "+updateCount+" rows updated [count="+batchExecCounter+"]");
					Util.logBatch.debug("executeBatch(): "+updateCount+" rows updated [count="+batchExecCounter+"; batchSize="+batchSize+"]");
				}
				else {
					logStmt.debug("addBatch() executed [count="+batchExecCounter+"]");
				}
			}
			else {
				//int urows = -1;
				if(usePreparedStatement){
					PreparedStatement stmt = conn.prepareStatement(stmtStr);
					try {
						setParameters(stmt);
						updateCount = stmt.executeUpdate();
						//if(log.isInfoEnabled()) { SQLUtils.logWarningsInfo(stmt.getWarnings(), log); }
					}
					finally {
						stmt.close();
					}
				}
				else {
					Statement stmt = conn.createStatement();
					try {
						updateCount = stmt.executeUpdate(replaceParameters(stmtStr));
						//if(log.isInfoEnabled()) { SQLUtils.logWarningsInfo(stmt.getWarnings(), log); }
					}
					finally {
						stmt.close();
					}
				}
				
				if(logStmt.isDebugEnabled()) {
					logStmt.debug("updated "+updateCount+" rows");
				}
				else if(logUpdates.isDebugEnabled() && updateCount>0) {
					logUpdates.debug("updated "+updateCount+" rows");
				}
			}
			if(useSavepoint) {
				ConnectionUtil.releaseSavepoint(conn, sp);
			}
			return updateCount;
		}
		catch(SQLException e) {
			//log.warn("error in stmt: "+stmtStr);
			if(rollbackOnError && !commitStrategy.equals(CommitStrategy.AUTO_COMMIT)) {
				ConnectionUtil.doRollback(conn, sp);
			}
			throw e;
		}
	}
	
	void setParameters(PreparedStatement stmt) throws SQLException {
		int i=1;
		while(true) {
			String key = Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_PARAM+"."+i;
			String param = papp.getProperty(key);
			if(param!=null) {
				log.debug("param #"+i+"/"+key+": "+param);
				stmt.setString(i, param);
			}
			else { return; }
			i++;
		}
	}

	static final String questionMarkPattern = Pattern.quote("?");
	
	String replaceParameters(String stmt) throws SQLException {
		int i=1;
		String retStmt = stmt;
		while(true) {
			String key = Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_PARAM+"."+i;
			String param = papp.getProperty(key);
			if(param!=null) {
				log.debug("param #"+i+"/"+key+": "+param);
				retStmt = retStmt.replaceFirst(questionMarkPattern, param);
			}
			else { return retStmt; }
			i++;
		}
	}

	void setupProperties() {
		if(papp==null) {
			log.warn("null properties!");
			return;
		}
		String execId = papp.getProperty(SQLRun.PROP_PROCID);
		String prefix = Constants.PREFIX_EXEC + execId + ".";
		useBatchUpdate = Utils.getPropBool(papp, prefix + Constants.SUFFIX_BATCH_MODE, DEFAULT_USE_BATCH_UPDATE);
		batchSize = Utils.getPropLong(papp, prefix + Constants.SUFFIX_BATCH_SIZE, DEFAULT_BATCH_SIZE);
		inputEncoding = papp.getProperty(prefix + Constants.SUFFIX_ENCODING, defaultInputEncoding);
		escapeBackslashedApos = Utils.getPropBool(papp, prefix + SUFFIX_ESCAPE_BACKSLASHED_APOS, DEFAULT_ESCAPE_BACKSLASHED_APOS);
		logEachXStmts = Utils.getPropLong(papp, prefix + Constants.SUFFIX_LOG_EACH_X_INPUT_ROWS, DEFAULT_LOG_EACH_X_INPUT_ROWS);
	}
	
	int closeStatement() throws SQLException {
		if(batchStmt!=null) {
			int[] updateCounts = batchStmt.executeBatch();
			int updateCount = MathUtil.sumInts(updateCounts);
			logStmt.debug("executeBatch(): "+updateCount+" rows updated");
			
			batchStmt.close(); batchStmt = null; batchExecCounter = 0;
			
			return updateCount;
		}
		return 0;
	}
	
	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void setProperties(Properties papp) {
		String tokenizer = papp.getProperty(StmtProc.PROP_SQLTOKENIZERCLASS);
		/*if(tokenizer!=null) {
			log.info("using sqltokenizerclass '"+tokenizer+"' [prop '"+StmtProc.PROP_SQLTOKENIZERCLASS+"']");
		}*/
		tokenizerStrategy = TokenizerStrategy.getTokenizerStrategy(tokenizer);
		usePreparedStatement = Utils.getPropBool(papp, PROP_USE_PREPARED_STATEMENT, usePreparedStatement);
		if(!usePreparedStatement) {
			log.info("not using prepared statements [prop '"+PROP_USE_PREPARED_STATEMENT+"']");
		}
		useSavepoint = Utils.getPropBool(papp, PROP_USE_SAVEPOINT, useSavepoint);
		
		this.papp = papp;
	}
	
	@Override
	public void setCommitStrategy(CommitStrategy commitStrategy) {
		this.commitStrategy = commitStrategy;
	}

	@Override
	public String getExecId() {
		return execId;
	}

	@Override
	public void setExecId(String execId) {
		this.execId = execId;
	}

	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		return ret;
	}

	@Override
	public List<String> getExecSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(EXEC_SUFFIXES));
		return ret;
	}

	@Override
	public void setDefaultFileEncoding(String encoding) {
		defaultInputEncoding = encoding;
	}
	
}
