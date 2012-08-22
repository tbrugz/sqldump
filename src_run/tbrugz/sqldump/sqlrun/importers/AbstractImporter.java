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
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.sqlrun.SQLRun;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.NonNullGetMap;

public abstract class AbstractImporter {
	public static class IOCounter {
		long input = 0;
		long output = 0;
	}

	static final Log log = LogFactory.getLog(AbstractImporter.class);

	Properties prop;
	Connection conn;
	
	String execId = null;
	String importFile = null;
	String importDir = null;
	String importFiles = null;
	boolean follow = false;
	String recordDelimiter = "\n";
	String insertTable = null;
	String insertSQL = null;
	String inputEncoding = "UTF-8";
	long sleepMilis = 100; //XXX: prop for sleepMilis (used in follow mode)?
	long skipHeaderN = 0;
	Long commitEachXrows = 100l;

	//needed as a property for 'follow' mode
	InputStream fileIS = null;
	PreparedStatement stmt = null;

	//prefix
	public static final String PREFIX_FAILOVER = ".failover.";

	//suffixes
	static final String SUFFIX_IMPORTFILE = ".importfile";
	static final String SUFFIX_IMPORTDIR = ".importdir";
	static final String SUFFIX_IMPORTFILES = ".importfiles";
	//XXX: static String SUFFIX_IMPORTFILES //??
	static final String SUFFIX_FOLLOW = ".follow";
	static final String SUFFIX_RECORDDELIMITER = ".recorddelimiter";
	static final String SUFFIX_ENCLOSING = ".enclosing";
	static final String SUFFIX_INSERTTABLE = ".inserttable";
	static final String SUFFIX_INSERTSQL = ".insertsql";
	static final String SUFFIX_ENCODING = ".encoding";
	static final String SUFFIX_SKIP_N = ".skipnlines";
	static final String SUFFIX_X_COMMIT_EACH_X_ROWS = ".x-commiteachxrows"; //XXX: to be overrided by SQLRun (CommitStrategy: STATEMENT, ...)?
	
	static final String[] AUX_SUFFIXES = {
		SUFFIX_ENCLOSING,
		SUFFIX_ENCODING,
		SUFFIX_FOLLOW,
		SUFFIX_IMPORTFILE,
		SUFFIX_IMPORTDIR,
		SUFFIX_IMPORTFILES,
		SUFFIX_INSERTTABLE,
		SUFFIX_INSERTSQL,
		SUFFIX_RECORDDELIMITER,
		SUFFIX_SKIP_N,
		SUFFIX_X_COMMIT_EACH_X_ROWS
	};
	
	public void setExecId(String execId) {
		this.execId = execId;
	}
	
	public void setProperties(Properties prop) {
		this.prop = prop;
		importFile = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTFILE);
		importDir = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTDIR);
		importFiles = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTFILES);
		inputEncoding = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_ENCODING, inputEncoding);
		recordDelimiter = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_RECORDDELIMITER, recordDelimiter);
		skipHeaderN = Utils.getPropLong(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_SKIP_N, skipHeaderN);
		follow = Utils.getPropBool(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_FOLLOW, follow);
		commitEachXrows = Utils.getPropLong(prop, SQLRun.PREFIX_EXEC+execId+SUFFIX_X_COMMIT_EACH_X_ROWS, commitEachXrows);
		
		setDefaultImporterProperties(prop);
	}

	void setDefaultImporterProperties(Properties prop) {
		setImporterProperties(prop, SQLRun.PREFIX_EXEC+execId);
	}
	
	void setImporterProperties(Properties prop, String importerPrefix) {
		insertTable = prop.getProperty(importerPrefix+SUFFIX_INSERTTABLE);
		insertSQL = prop.getProperty(importerPrefix+SUFFIX_INSERTSQL);
		//XXX: mustSetupSQLStatement = true ?
	}

	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(AUX_SUFFIXES));
		return ret;
	}

	//XXX add countsByFailoverId for all files?
	public long importData() throws SQLException, InterruptedException, IOException {
		long ret = 0;
		if(importFile!=null) {
			ret = importFile();
		}
		else if(importFiles!=null) {
			if(importDir==null) {
				importDir = System.getProperty("user.dir");
			}
			log.info("importing files from dir: "+importDir);
			List<String> files = SQLRun.getFiles(importDir, importFiles);
			for(String file: files) {
				importFile = file;
				//log.info("importing file: "+importFile);
				ret += importFile();
			}
		}
		else {
			log.warn("neither '"+SUFFIX_IMPORTFILE+"' nor '"+SUFFIX_IMPORTFILES+"' suffix specified...");
		}
		log.info("imported lines = "+ret);
		return ret;
	}

	Map<Integer, IOCounter> countsByFailoverId;
	List<Integer> filecol2tabcolMap = null;
	boolean mustSetupSQLStatement = false;
	int failoverId = 0;
	
	//TODOne: show processedLines by failoverId?
	long importFile() throws SQLException, InterruptedException, IOException {
		//init counters
		countsByFailoverId = new NonNullGetMap<Integer, IOCounter>(IOCounter.class);
		IOCounter counter = countsByFailoverId.get(failoverId);

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
		//int[] filecol2tabcolMap = null;
		do {
			while(scan.hasNext()) {
				boolean importthisline = true;
				if(failoverId>0) {
					failoverId = 0;
					counter = countsByFailoverId.get(failoverId);
					setDefaultImporterProperties(prop);
					mustSetupSQLStatement = true;
				}
				String line = scan.next();
				
				while(importthisline) {
					try {
						procLineInternal(line, is1stloop);
						importthisline = false;
					}
					catch(Exception e) {
						counter.input++;
						failoverId++;
						counter = countsByFailoverId.get(failoverId);
						String failoverKey = SQLRun.PREFIX_EXEC+execId+PREFIX_FAILOVER+failoverId;
						List<String> foids = Utils.getKeysStartingWith(prop, failoverKey);
						if(foids!=null && foids.size()>0) {
							//log.info("failover["+failoverId+"]: "+failoverKey);
							setImporterProperties(prop, failoverKey);
							mustSetupSQLStatement = true;
						}
						else {
							log.warn("error processing line "+counter.input+": "+e.getMessage());
							importthisline = false;
							break;
						}
					}
				} //while (importthisline)
				
			}
			//XXX: commit in follow mode? here?
			//XXX: sleep only in follow mode?
			if(follow) { Thread.sleep(sleepMilis); }
			if(fileIS!=null && fileIS.available()>0) {
				//log.debug("avaiable: "+fileIS.available());
				scan = createScanner(); 
			}
			is1stloop = false;
		}
		while(follow);
		
		fileIS.close(); fileIS = null;

		//show counters
		long countAll = 0;
		for(Integer id: countsByFailoverId.keySet()) {
			IOCounter cc = countsByFailoverId.get(id);
			if(cc.input>0 || cc.output>0) {
				log.info((id==0?"":"[failover="+id+"] ")+"processedLines: "+cc.input+" ; importedRows: "+cc.output);
				countAll += cc.output;
			}
		}
		
		return countAll;
	}
	
	void procLineInternal(String line, boolean is1stloop) throws SQLException {
		//log.info("line["+processedLines+"]: "+line);
		IOCounter counter = countsByFailoverId.get(failoverId);
		String[] parts = procLine(line, counter.input);
		if(parts==null) {
			log.debug("line could not be understood: "+line);
			throw new RuntimeException("line could not be processed: "+line);
		}
		
		if(log.isDebugEnabled()) {
			log.debug("parts["+counter.input+"; l="+parts.length+"]: "+Arrays.asList(parts));
		}
		
		if(counter.input==0 || mustSetupSQLStatement ) {
			setupSQLStatement(parts);
			mustSetupSQLStatement = false;
		}
		if(is1stloop && skipHeaderN>counter.input) {
			counter.input++;
			return;
		}
		
		for(int i=0;i<parts.length;i++) {
			if(filecol2tabcolMap!=null) {
				//log.info("v: "+i);
				if(filecol2tabcolMap.contains(i)) {
					int index = filecol2tabcolMap.indexOf(i);
					stmt.setString(index+1, parts[i]);
					//log.info("v: "+i+" / "+index+"~"+(index+1)+" / "+parts[index]+" // "+parts[i]);						
				}
			}
			else {
				stmt.setString(i+1, parts[i]);
			}
			//stmtStr = stmtStrPrep.replaceFirst("\\?", parts[i]);
		}
		//log.info("insert["+processedLines+"/"+importedLines+"]: "+stmtStr);
		//stmt.addBatch(); //XXX: batch insert?
		int changedRows = stmt.executeUpdate();
		counter.input++;
		counter.output += changedRows;
		if(commitEachXrows!=null && commitEachXrows>0 && (counter.output%commitEachXrows==0)) {
			doCommit(conn);
		}
	}
	
	List<Integer> loggedStatementFailoverIds = new ArrayList<Integer>();
	
	//TODO: map of statements (one for each failoverId)?
	void setupSQLStatement(String[] parts) throws SQLException {
		StringBuffer sb = new StringBuffer();
		if(insertSQL!=null) {
			log.debug("original insert sql: "+insertSQL);
			filecol2tabcolMap = new ArrayList<Integer>();
			int fromIndex = 0;
			while(true) {
				int ind1 = insertSQL.indexOf("${", fromIndex);
				//int indQuestion = insertSQL.indexOf("?", fromIndex);
				if(ind1<0) { break; }
				int ind2 = insertSQL.indexOf("}", ind1);
				//log.debug("ind/2: "+ind+" / "+ind2);
				int number = Integer.parseInt(insertSQL.substring(ind1+2, ind2));
				fromIndex = ind2;
				filecol2tabcolMap.add(number);
			}
			//XXX: mix ? and ${number} ?
			//filecol2tabcolMap = new int[parts.length];
			//for(int i=0;i<intl.size();i++) { filecol2tabcolMap[i] = intl.get(0); }
			String thisInsertSQL = insertSQL.replaceAll("\\$\\{[0-9]+\\}", "?");
			if(filecol2tabcolMap.size()>0) {
				log.debug("mapper: "+filecol2tabcolMap);
			}
			sb.append(thisInsertSQL);
		}
		else {
			sb.append("insert into "+insertTable+ " values (");
			for(int i=0;i<parts.length;i++) {
				sb.append((i==0?"":", ")+"?");
			}
			sb.append(")");
		}
		if(!loggedStatementFailoverIds.contains(failoverId)) {
			log.info("insert sql"+(failoverId>0?"[failover="+failoverId+"]":"")+": "+sb.toString());
			loggedStatementFailoverIds.add(failoverId);
		}
		stmt = conn.prepareStatement(sb.toString());
		//stmtStrPrep = sb.toString();
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
