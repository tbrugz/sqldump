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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.ProcessingException;
import tbrugz.sqldump.def.AbstractFailable;
import tbrugz.sqldump.sqlrun.SQLRun.CommitStrategy;
import tbrugz.sqldump.util.IOUtil;
import tbrugz.sqldump.util.Utils;

public class StmtProc extends AbstractFailable implements Executor {
	static Log log = LogFactory.getLog(StmtProc.class);
	static Log logRow = LogFactory.getLog(StmtProc.class.getName()+"-row");
	static Log logStmt = LogFactory.getLog(StmtProc.class.getName()+"-stmt");
	
	boolean useBatchUpdate = false;
	long batchSize = 1000;
	boolean useSQLStmtTokenizerClass = true;
	
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

	public void execFile(String filePath, String errorLogKey, boolean split) throws IOException {
		setupProperties();
		//String errorLogFilePath = papp.getProperty(errorLogKey);
		File file = new File(filePath);
		FileReader reader = new FileReader(file);
		Writer logerror = null;
		String fileStr = IOUtil.readFile(reader);
		//FIXME: SQLStmtTokenizer not working (on big files?)
		Iterable<String> stmtTokenizer = null;
		if(useSQLStmtTokenizerClass) {
			stmtTokenizer = new SQLStmtTokenizer(fileStr);
		}
		else {
			stmtTokenizer = new StringSpliter(fileStr, split);
		}
		reader.close();
		
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
			stmtStr = stmtStr.trim();
			if(stmtStr.equals("")) { continue; }
			
			try {
				urowsTotal += execStatementInternal(stmtStr);
				countOk++;
			}
			catch(SQLException e) {
				logStmt.warn("error executing updates [stmt = "+stmtStr+"]: "+e);
				if(logerror==null) {
					logerror = getErrorLogHandler(errorLogKey);
				}
				if(logerror!=null) {
					logerror.write(stmtStr+";\n");
				}
				countError++;
				logStmt.debug("error executing updates", e);
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
		try {
			statementsPerSec = ((double) countExec) / ( ((double) totalTime) / 1000 );
		}
		catch(ArithmeticException e) {}
		
		log.info("exec = "+countExec+" [ok = "+countOk+", error = "+countError+"], rows updated = "+urowsTotal
				+", elapsed = "+totalTime+"ms, statements/sec = "+statementsPerSec
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
		catch (NullPointerException npe) {
			log.warn("error log file not defined. Ex: "+npe);
			//npe.printStackTrace();
			errorFileNotFoundWarned = true;
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
			if(failonerror) { throw new ProcessingException(e); }
			return 0;
		}
	}
	
	int execStatementInternal(String stmtStr) throws IOException, SQLException {
		if(stmtStr==null) { throw new IllegalArgumentException("null parameter"); }
		stmtStr = stmtStr.trim();
		if(stmtStr.equals("")) { throw new IllegalArgumentException("null parameter"); }
		
		logStmt.debug("executing sql: "+stmtStr);
		try {
			if(useBatchUpdate) {
				if(batchStmt==null) {
					batchStmt = conn.createStatement();
				}
				batchStmt.addBatch(replaceParameters(stmtStr));
				batchExecCounter++;
				
				if((batchExecCounter%batchSize)==0) {
					int[] updateCounts = batchStmt.executeBatch();
					int updateCount = Util.sumInts(updateCounts);
					logStmt.debug("executeBatch(): "+updateCount+" rows updated [count="+batchExecCounter+"]");
					SQLRun.logBatch.debug("executeBatch(): "+updateCount+" rows updated [count="+batchExecCounter+"; batchSize="+batchSize+"]");
					return updateCount;
				}
				else {
					logStmt.debug("addBatch() executed [count="+batchExecCounter+"]");
					return 0;
				}
			}
			else {
				PreparedStatement stmt = conn.prepareStatement(stmtStr);
				setParameters(stmt);
				int urows = stmt.executeUpdate();
				logStmt.debug("updated "+urows+" rows");
				return urows;
			}
		}
		catch(SQLException e) {
			try {
				conn.rollback();
			}
			catch(SQLException e2) {
				log.warn("error in rollback: "+e2);
			}
			throw e;
		}
	}
	
	void setParameters(PreparedStatement stmt) throws SQLException {
		int i=1;
		while(true) {
			String key = SQLRun.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_PARAM+"."+i;
			String param = papp.getProperty(key);
			if(param!=null) {
				log.debug("param #"+i+"/"+key+": "+param);
				stmt.setString(i, param);
			}
			else { return; }
			i++;
		}
	}

	String replaceParameters(String stmt) throws SQLException {
		int i=1;
		String retStmt = stmt;
		while(true) {
			String key = SQLRun.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_PARAM+"."+i;
			String param = papp.getProperty(key);
			if(param!=null) {
				log.debug("param #"+i+"/"+key+": "+param);
				retStmt = retStmt.replaceFirst("?", param);
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
		useBatchUpdate = Utils.getPropBool(papp, SQLRun.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_BATCH_MODE, useBatchUpdate);
		batchSize = Utils.getPropLong(papp, SQLRun.PREFIX_EXEC+papp.getProperty(SQLRun.PROP_PROCID)+SQLRun.SUFFIX_BATCH_SIZE, batchSize);
	}
	
	int closeStatement() throws SQLException {
		if(batchStmt!=null) {
			int[] updateCounts = batchStmt.executeBatch();
			int updateCount = Util.sumInts(updateCounts);
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

	static final String STMT_TOKENIZER_CLASS = "SQLStmtTokenizer";
	static final String STRING_SPLITTER_CLASS = "StringSpliter";
	
	@Override
	public void setProperties(Properties papp) {
		String tokenizer = papp.getProperty(SQLRun.PROP_SQLTOKENIZERCLASS);
		useSQLStmtTokenizerClass = true;
		if(null == tokenizer) {
		}
		else if(STMT_TOKENIZER_CLASS.equals(tokenizer)) {
			log.info("using '"+tokenizer+"' tokenizer class");
		}
		else if(STRING_SPLITTER_CLASS.equals(tokenizer)) {
			useSQLStmtTokenizerClass = false;
			log.info("using '"+tokenizer+"' tokenizer class");
		}
		else {
			log.warn("unknown string tokenizer class: "+tokenizer+" (using '"+STMT_TOKENIZER_CLASS+"')");
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getExecSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(EXEC_SUFFIXES));
		return ret;
	}
	
}
