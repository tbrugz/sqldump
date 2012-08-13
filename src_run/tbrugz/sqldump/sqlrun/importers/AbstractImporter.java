package tbrugz.sqldump.sqlrun.importers;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.Utils;

public abstract class AbstractImporter {
	static final Log log = LogFactory.getLog(AbstractImporter.class);

	Properties prop;
	Connection conn;
	
	String execId = null;
	String importFile = null;
	boolean follow = false;
	String recordDelimiter = "\n";
	String insertTable = null;
	String inputEncoding = "UTF-8";
	long sleepMilis = 1000; //XXX: prop for sleepMilis (used in follow mode)?
	long skipHeaderN = 0;
	Long commitEachXrows = 100l;

	//needed as a property for 'follow' mode
	InputStream fileIS = null;
	PreparedStatement stmt = null;
	
	static String SUFFIX_IMPORTFILE = ".importfile";
	//XXX: static String SUFFIX_IMPORTFILES //??
	static String SUFFIX_FOLLOW = ".follow";
	static String SUFFIX_RECORDDELIMITER = ".recorddelimiter";
	static String SUFFIX_ENCLOSING = ".enclosing";
	static String SUFFIX_INSERTTABLE = ".inserttable";
	static String SUFFIX_ENCODING = ".encoding";
	static String SUFFIX_SKIP_N = ".skipnlines";
	static final String SUFFIX_X_COMMIT_EACH_X_ROWS = ".x-commiteachxrows"; //XXX: to be overrided by SQLRun (CommitStrategy: STATEMENT, ...)?
	
	static final String[] AUX_SUFFIXES = {
		SUFFIX_ENCLOSING,
		SUFFIX_ENCODING,
		SUFFIX_FOLLOW,
		SUFFIX_IMPORTFILE,
		SUFFIX_INSERTTABLE,
		SUFFIX_RECORDDELIMITER,
		SUFFIX_SKIP_N,
		SUFFIX_X_COMMIT_EACH_X_ROWS
	};
	
	public void setExecId(String execId) {
		this.execId = execId;
	}
	
	public void setProperties(Properties prop) {
		this.prop = prop;
		insertTable = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_INSERTTABLE);
		importFile = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTFILE);
		inputEncoding = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_ENCODING, inputEncoding);
		recordDelimiter = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_RECORDDELIMITER, recordDelimiter);
		skipHeaderN = Utils.getPropLong(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_SKIP_N, skipHeaderN);
		follow = Utils.getPropBool(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_FOLLOW, follow);
		commitEachXrows = Utils.getPropLong(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_X_COMMIT_EACH_X_ROWS, commitEachXrows);
	}
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(AUX_SUFFIXES));
		return ret;
	}

	public long importData() throws SQLException, InterruptedException, IOException {
		long processedLines = 0;
		long importedLines = 0;
		//assume all lines of same size (in number of columns?)
		//FileReader fr = new FileReader(importFile);
		
		Scanner scan = createScanner();
		
		Pattern p = scan.delimiter();
		log.debug("scan delimiter pattern: "+p);
		log.info("input file: "+importFile);
		
		//PreparedStatement stmt = null;
		//String stmtStrPrep = null;
		//String stmtStr = null;
		
		if(follow) {
			//add shutdown hook
			log.info("adding shutdown hook...");
			Runtime.getRuntime().addShutdownHook(new Thread(){
				public void run() {
					log.info("commiting & shutting down...");
					System.err.println("commiting & shutting down...");
					try {
						conn.commit();
					} catch (SQLException e) {
						log.warn("error commiting: "+e);
						System.err.println("error commiting: "+e);
					}
					log.info("shutting down");
					System.err.println("shutting down");
				}
			});
		}
		
		boolean is1stloop = true;
		do {
			
		while(scan.hasNext()) {
			String line = scan.next();
			//log.info("line["+processedLines+"]: "+line);
			String[] parts = procLine(line, processedLines);
			log.debug("parts["+parts.length+"]: "+Arrays.asList(parts));
			if(processedLines==0) {
				StringBuffer sb = new StringBuffer();
				sb.append("insert into "+insertTable+ " values (");
				for(int i=0;i<parts.length;i++) {
					sb.append((i==0?"":", ")+"?");
				}
				sb.append(")");
				stmt = conn.prepareStatement(sb.toString());
				//stmtStrPrep = sb.toString();
			}
			if(is1stloop && skipHeaderN>processedLines) {
				processedLines++;
				continue;
			}
			
			for(int i=0;i<parts.length;i++) {
				stmt.setString(i+1, parts[i]);
				//stmtStr = stmtStrPrep.replaceFirst("\\?", parts[i]);
			}
			//log.info("insert["+processedLines+"/"+importedLines+"]: "+stmtStr);
			//stmt.addBatch(); //XXX: batch insert?
			int changedRows = stmt.executeUpdate();
			processedLines++;
			importedLines += changedRows;
			if(commitEachXrows!=null && commitEachXrows>0 && (importedLines%commitEachXrows==0)) {
				doCommit(conn);
			}
		}
		//XXX: commit in follow mode? here?
		Thread.sleep(sleepMilis);
		if(fileIS!=null && fileIS.available()>0) {
			//log.debug("avaiable: "+fileIS.available());
			scan = createScanner(); 
		}
		is1stloop = false;
		}
		while(follow);
		
		log.info("processedLines: "+processedLines+" ; importedRows: "+importedLines);
		
		return processedLines;
	}

	abstract String[] procLine(String line, long processedLines) throws SQLException;
	
	Scanner createScanner() throws FileNotFoundException {
		Scanner scan = null;
		if(SQLRun.STDIN.equals(importFile)) {
			scan = new Scanner(System.in, inputEncoding);
		}
		else {
			if(fileIS==null) {
				fileIS = new BufferedInputStream(new FileInputStream(importFile));
			}
			scan = new Scanner(fileIS, inputEncoding);
			//scan = new Scanner(new File(importFile), inputEncoding);
		}
		scan.useDelimiter(Pattern.quote(recordDelimiter));
		return scan;
	}
	
	static void doCommit(Connection conn) {
		try {
			conn.commit();
		} catch (SQLException e) {
			log.warn("error commiting: "+e);
		}
	}
}
