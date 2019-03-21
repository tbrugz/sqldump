package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtNgScanner;
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtScanner;
import tbrugz.sqldump.sqlrun.tokenzr.SQLStmtTokenizer;
import tbrugz.sqldump.sqlrun.tokenzr.StringSpliter;
import tbrugz.sqldump.util.ConnectionUtil;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.MathUtil;
import tbrugz.sqldump.util.ParametrizedProperties;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringUtils;
import tbrugz.sqldump.util.Utils;

//XXX: remove references to SQLRun class
@SuppressWarnings("deprecation")
public class StmtProc extends AbstractFailable implements Executor {
	static final Log log = LogFactory.getLog(StmtProc.class);
	static final Log logRow = LogFactory.getLog(StmtProc.class.getName()+"-row");
	static final Log logStmt = LogFactory.getLog(StmtProc.class.getName()+"-stmt");
	static final Log logUpdates = LogFactory.getLog(StmtProc.class.getName()+"-updates");
	static final Log logInner = LogFactory.getLog(StmtProc.class.getName()+"-inner");
	
	//properties
	static final String PROP_SQLTOKENIZERCLASS = "sqlrun.sqltokenizerclass";
	static final String PROP_USE_PREPARED_STATEMENT = "sqlrun.usepreparedstatement";
	static final String SUFFIX_ESCAPE_BACKSLASHED_APOS = ".escapebackslashedapos";
	
	boolean useBatchUpdate = false;
	boolean usePreparedStatement = true;
	boolean escapeBackslashedApos = false;
	boolean replacePropsOnFileContents = true; //XXX: add prop for 'replacePropsOnFileContents'
	boolean rollbackOnError = true;
	
	long batchSize = 1000;
	String defaultInputEncoding = DataDumpUtils.CHARSET_UTF8;
	String inputEncoding = defaultInputEncoding;
	
	public enum TokenizerStrategy {
		STMT_TOKENIZER,
		STMT_SCANNER,
		STMT_SCANNER_NG,
		STRING_SPLITTER;
		
		public static final String STMT_TOKENIZER_CLASS = "SQLStmtTokenizer";
		public static final String STRING_SPLITTER_CLASS = "StringSpliter";
		public static final String STMT_SCANNER_CLASS = "SQLStmtScanner";
		public static final String STMT_SCANNER_NG_CLASS = "SQLStmtNgScanner";
		
		public static TokenizerStrategy getTokenizer(String tokenizer) {
			if(tokenizer == null) {
				return TokenizerStrategy.STMT_SCANNER;
			}
			tokenizer = tokenizer.trim();

			if(STMT_TOKENIZER_CLASS.equals(tokenizer)) {
				log.info("using '"+tokenizer+"' tokenizer class");
				return TokenizerStrategy.STMT_TOKENIZER;
			}
			else if(STRING_SPLITTER_CLASS.equals(tokenizer)) {
				log.warn("using deprecated '"+tokenizer+"' tokenizer class");
				return TokenizerStrategy.STRING_SPLITTER;
			}
			else if(STMT_SCANNER_CLASS.equals(tokenizer)) {
				log.info("using '"+tokenizer+"' tokenizer class");
				return TokenizerStrategy.STMT_SCANNER;
			}
			else if(STMT_SCANNER_NG_CLASS.equals(tokenizer)) {
				log.info("using '"+tokenizer+"' tokenizer class");
				return TokenizerStrategy.STMT_SCANNER_NG;
			}
			else {
				throw new IllegalArgumentException("unknown string tokenizer class: "+tokenizer);
			}
		}
	}
	
	TokenizerStrategy tokenizerStrategy = TokenizerStrategy.STMT_SCANNER;
	
	Connection conn;
	Properties papp;
	//CommitStrategy commitStrategy;
	
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
		Iterable<String> stmtTokenizer = null;
		switch(tokenizerStrategy) {
		case STMT_SCANNER_NG:
			//XXX option to define charset
			stmtTokenizer = new SQLStmtNgScanner(file, inputEncoding);
			break;
		case STMT_SCANNER:
			//XXX option to define charset
			stmtTokenizer = new SQLStmtScanner(file, inputEncoding, escapeBackslashedApos);
			break;
		default:
			FileReader reader = new FileReader(file);
			String fileStr = IOUtil.readFromReader(reader);
			switch (tokenizerStrategy) {
			case STMT_TOKENIZER:
				stmtTokenizer = new SQLStmtTokenizer(fileStr);
				break;
			case STRING_SPLITTER:
				stmtTokenizer = new StringSpliter(fileStr, split);
				break;
			default:
				throw new IllegalStateException("unknown TokenizerStrategy: "+tokenizerStrategy);
			}
			reader.close();
		}

		Writer logerror = null;
		
		log.info("file exec: statements from file '"+file+"'...");
		long logEachXStmts = 1000;
		long urowsTotal = 0;
		long countOk = 0;
		long countError = 0;
		long countExec = 0;
		//boolean errorFileNotFoundWarned = false;
		long initTime = System.currentTimeMillis();
		
		for(String stmtStr: stmtTokenizer) {
			if(stmtStr==null) { continue; }
			//XXX: remove SQL comments before trim()?
			stmtStr = stmtStr.trim();
			if(stmtStr.equals("")) { continue; }
			
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
		if(rollbackOnError) {
			// XXX see conn.getMetaData().supportsSavepoints()
			sp = conn.setSavepoint();
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
			ConnectionUtil.releaseSavepoint(conn, sp);
			return updateCount;
		}
		catch(SQLException e) {
			//log.warn("error in stmt: "+stmtStr);
			if(rollbackOnError) {
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
		//TODO: useBatchUpdate & batchSize: default value should not be based on previous value
		useBatchUpdate = Utils.getPropBool(papp, Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+Constants.SUFFIX_BATCH_MODE, useBatchUpdate);
		batchSize = Utils.getPropLong(papp, Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+Constants.SUFFIX_BATCH_SIZE, batchSize);
		inputEncoding = papp.getProperty(Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+Constants.SUFFIX_ENCODING, defaultInputEncoding);
		escapeBackslashedApos = Utils.getPropBool(papp, Constants.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SUFFIX_ESCAPE_BACKSLASHED_APOS, escapeBackslashedApos);
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
		tokenizerStrategy = TokenizerStrategy.getTokenizer(tokenizer);
		usePreparedStatement = Utils.getPropBool(papp, PROP_USE_PREPARED_STATEMENT, usePreparedStatement);
		if(!usePreparedStatement) {
			log.info("not using prepared statements [prop '"+PROP_USE_PREPARED_STATEMENT+"']");
		}
		
		this.papp = papp;
	}
	
	@Override
	public void setCommitStrategy(CommitStrategy commitStrategy) {
		//XXX this.commitStrategy = commitStrategy;
	}

	@Override
	public void setExecId(String execId) {
		// TODO Auto-generated method stub
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
