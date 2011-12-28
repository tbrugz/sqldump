package tbrugz.sqldump.sqlregen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import tbrugz.sqldump.ParametrizedProperties;
import tbrugz.sqldump.SQLDump;
import tbrugz.sqldump.Utils;

import static tbrugz.sqldump.SQLDump.*; 

/*
 * TODO: cli (sqlplus-like)? continue/exit on error?
 * XXX: show/log ordered 'exec-ids' before executing
 * XXX: option for numeric/alphanumeric proc-ids
 * XXX: log total running time
 */
public class SQLRegen {
	
	static Logger log = Logger.getLogger(SQLRegen.class);
	
	static final String PROPERTIES_FILENAME = "sqlregen.properties";
	
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
		for(String key: execkeys) {
			String procId = getExecId(key, PREFIX_EXEC);
			
			// .file
			if(key.endsWith(SUFFIX_FILE)) {
				log.info(">>> processing: id = '"+procId+"'");
				//String execId = getExecId(key, PREFIX_EXEC, SUFFIX_FILE);
				//log.info("processing: exec-id = "+execId);
				try {
					execFile(key, PREFIX_EXEC+procId+SUFFIX_LOGINVALIDSTATEMENTS);
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
					int urows = execStatement(stmtStr);
					log.info("statement exec: updates = "+urows);
					log.debug("statement: "+stmtStr);
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
	}

	void execFile(String fileKey, String errorLogKey) throws IOException, SQLException {
		String filePath = papp.getProperty(fileKey);
		String errorLogFilePath = papp.getProperty(errorLogKey);
		FileReader reader = new FileReader(filePath);
		FileWriter logerror = null;
		String fileStr = IOUtil.readFile(reader);
		String[] statementStrs = fileStr.split(";");
		reader.close();
		
		log.info("exec "+statementStrs.length+" statements from file '"+filePath+"'...");
		long logEachXStmts = 1000;
		long urowsTotal = 0;
		long countOk = 0;
		long countError = 0;
		long countExec = 0;
		long initTime = System.currentTimeMillis();
		for(String stmtStr: statementStrs) {
			if(stmtStr==null) { continue; }
			stmtStr = stmtStr.trim();
			if(stmtStr.equals("")) { continue; }
			
			try {
				urowsTotal += execStatement(stmtStr);
				countOk++;
			}
			catch(SQLException e) {
				log.warn("error executing updates [stmt = "+stmtStr+"]: "+e);
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
						log.warn("error opening file '"+errorLogFilePath+"' for writing invalid statements. Ex: "+fnfe);
					}
				}
				logerror.write(stmtStr+";\n");
				countError++;
				log.debug("error executing updates", e);
			}
			countExec++;
			
			if((countExec>0) && (countExec % logEachXStmts)==0) {
				log.info(countExec+" statements processed");
			}
		}
		long totalTime = System.currentTimeMillis() - initTime;
		//commit?
		double statementsPerSec = Double.NaN;
		try {
			statementsPerSec = ((double) countExec) / ( ((double) totalTime) / 1000 );
		}
		catch(ArithmeticException e) {}
		
		log.info("exec = "+countExec+", ok = "+countOk+", error = "+countError+", updates = "+urowsTotal
				+", elapsed = "+totalTime+"ms, statements/sec = "+statementsPerSec
				+" [file = '"+filePath+"']");
		if(logerror!=null) {
			logerror.close();
			log.warn(""+countError+" erroneous statements in '"+errorLogFilePath+"'");
		}
	}
	
	int execStatement(String stmtStr) throws IOException, SQLException {
		if(stmtStr==null) { throw new IllegalArgumentException("null parameter"); }
		stmtStr = stmtStr.trim();
		if(stmtStr.equals("")) { throw new IllegalArgumentException("null parameter"); }
		
		Statement stmt = conn.createStatement();
		log.debug("executing sql: "+stmtStr);
		int urows = stmt.executeUpdate(stmtStr);
		log.debug("updated "+urows+" rows");
		return urows;
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
			sqlr.conn = SQLDump.initDBConnection(args, sqlr.papp);
			sqlr.doIt();
		}
		finally {
			sqlr.end();
		}
	}

}
