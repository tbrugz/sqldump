package tbrugz.sqldump.sqlregen;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.IOUtil;

public class StmtProc {
	static Logger log = Logger.getLogger(StmtProc.class);
	static Logger logRow = Logger.getLogger(StmtProc.class.getName()+"-row");
	static Logger logStmt = Logger.getLogger(StmtProc.class.getName()+"-stmt");
	
	Connection conn;
	Properties papp;
	
	public void execFile(String fileKey, String errorLogKey) throws IOException, SQLException {
		String filePath = papp.getProperty(fileKey);
		String errorLogFilePath = papp.getProperty(errorLogKey);
		FileReader reader = new FileReader(filePath);
		FileWriter logerror = null;
		String fileStr = IOUtil.readFile(reader);
		String[] statementStrs = fileStr.split(";");
		reader.close();
		
		log.info("file exec: "+statementStrs.length+" statements from file '"+filePath+"'...");
		long logEachXStmts = 1000;
		long urowsTotal = 0;
		long countOk = 0;
		long countError = 0;
		long countExec = 0;
		boolean errorFileNotFoundWarned = false;
		long initTime = System.currentTimeMillis();
		
		for(String stmtStr: statementStrs) {
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
					try {
						File f = new File(errorLogFilePath);
						File dir = f.getParentFile();
						if(!dir.isDirectory()) {
							log.info("creating dir: "+dir);
							dir.mkdirs();
						}
						logerror = new FileWriter(errorLogFilePath);
					}
					catch(FileNotFoundException fnfe) {
						if(!errorFileNotFoundWarned) {
							log.warn("error opening file '"+errorLogFilePath+"' for writing invalid statements. Ex: "+fnfe);
							errorFileNotFoundWarned = true;
						}
					}
				}
				logerror.write(stmtStr+";\n");
				countError++;
				logStmt.debug("error executing updates", e);
			}
			countExec++;
			
			if((countExec>0) && (countExec % logEachXStmts)==0) {
				logRow.info(countExec+" statements processed");
			}
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
				+" [file = '"+filePath+"']");
		if(logerror!=null) {
			logerror.close();
			log.warn(""+countError+" erroneous statements in '"+errorLogFilePath+"'");
		}
	}
	
	public int execStatement(String stmtStr) throws IOException, SQLException {
		int urows = execStatementInternal(stmtStr);
		log.info("statement exec: updates = "+urows);
		log.debug("statement: "+stmtStr);
		return urows;
	}
	
	int execStatementInternal(String stmtStr) throws IOException, SQLException {
		if(stmtStr==null) { throw new IllegalArgumentException("null parameter"); }
		stmtStr = stmtStr.trim();
		if(stmtStr.equals("")) { throw new IllegalArgumentException("null parameter"); }
		
		Statement stmt = conn.createStatement();
		logStmt.debug("executing sql: "+stmtStr);
		int urows = stmt.executeUpdate(stmtStr);
		logStmt.debug("updated "+urows+" rows");
		return urows;
	}
	
	public Connection getConn() {
		return conn;
	}
	public void setConn(Connection conn) {
		this.conn = conn;
	}
	public Properties getPapp() {
		return papp;
	}
	public void setPapp(Properties papp) {
		this.papp = papp;
	}
	
}
