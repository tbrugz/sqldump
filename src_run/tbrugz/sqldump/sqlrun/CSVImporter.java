package tbrugz.sqldump.sqlrun;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.util.Utils;

public class CSVImporter {
	static Log log = LogFactory.getLog(CSVImporter.class);

	Properties prop;
	Connection conn;
	
	String execId = null;
	String importFile = null;
	boolean follow = false;
	String columnDelimiter = ",";
	String recordDelimiter = "\n";
	String insertTable = null;
	String inputEncoding = "UTF-8";
	long sleepMilis = 1000; //XXX: prop for sleepMilis (used in follow mode)?
	long skipHeaderN = 0;
	Long commitEachXrows = 100l;

	static String SUFFIX_IMPORTFILE = ".importfile";
	//XXX: static String SUFFIX_IMPORTFILES //??
	static String SUFFIX_FOLLOW = ".follow";
	static String SUFFIX_COLUMNDELIMITER = ".columndelimiter";
	static String SUFFIX_RECORDDELIMITER = ".recorddelimiter";
	static String SUFFIX_ENCLOSING = ".enclosing";
	static String SUFFIX_INSERTTABLE = ".inserttable";
	static String SUFFIX_ENCODING = ".encoding";
	static String SUFFIX_SKIP_N = ".skipnlines";
	
	static String[] CSV_AUX_SUFFIXES = {
		SUFFIX_COLUMNDELIMITER,
		SUFFIX_ENCLOSING,
		SUFFIX_ENCODING,
		SUFFIX_FOLLOW,
		SUFFIX_IMPORTFILE,
		SUFFIX_INSERTTABLE,
		SUFFIX_RECORDDELIMITER,
		SUFFIX_SKIP_N
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
		columnDelimiter = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_COLUMNDELIMITER, columnDelimiter);
		skipHeaderN = Utils.getPropLong(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_SKIP_N, skipHeaderN);
		follow = Utils.getPropBool(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_FOLLOW, follow);
	}
	
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public String[] getAuxSuffixes() {
		return CSV_AUX_SUFFIXES;
	}

	public long importData() throws FileNotFoundException, SQLException, InterruptedException {
		long processedLines = 0;
		long importedLines = 0;
		//assume all lines of same size (in number of columns?)
		//FileReader fr = new FileReader(importFile);
		
		Scanner scan = null;
		if(SQLRun.STDIN.equals(importFile)) {
			scan = new Scanner(System.in, inputEncoding);
		}
		else {
			scan = new Scanner(new File(importFile), inputEncoding);
		}
		//default scanner delimiter pattern: \p{javaWhitespace}+
		//scan.useDelimiter("\\n");
		scan.useDelimiter(Matcher.quoteReplacement(recordDelimiter));
		
		Pattern p = scan.delimiter();
		log.debug("scan delimiter pattern: "+p);
		log.info("input file: "+importFile);
		
		PreparedStatement stmt = null;
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
		
		do {
			
		while(scan.hasNext()) {
			String line = scan.next();
			//log.info("line["+processedLines+"]: "+line);
			String[] parts = line.split(columnDelimiter);
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
			//log.info("parts: "+Arrays.asList(parts));
			if(skipHeaderN>processedLines) {
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
		}
		while(follow);
		
		log.info("processedLines: "+processedLines+" ; importedRows: "+importedLines);
		
		return processedLines;
	}
	
	static void doCommit(Connection conn) {
		try {
			conn.commit();
		} catch (SQLException e) {
			log.warn("error commiting: "+e);
		}
	}
}
