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
		insertTable = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_INSERTTABLE);
		insertSQL = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_INSERTSQL);
		importFile = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTFILE);
		importDir = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTDIR);
		importFiles = prop.getProperty(SQLRun.PREFIX_EXEC+execId+SUFFIX_IMPORTFILES);
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
				log.info("importing file: "+importFile);
				ret += importFile();
			}
		}
		else {
			log.warn("neither '"+SUFFIX_IMPORTFILE+"' nor '"+SUFFIX_IMPORTFILES+"' suffix specified...");
		}
		log.info("imported lines = "+ret);
		return ret;
	}
	
	long importFile() throws SQLException, InterruptedException, IOException {
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
		//int[] filecol2tabcolMap = null;
		List<Integer> filecol2tabcolMap = null;
		do {
			
		while(scan.hasNext()) {
			String line = scan.next();
			//log.info("line["+processedLines+"]: "+line);
			String[] parts = procLine(line, processedLines);
			if(parts==null) {
				log.warn("line could not be processed: "+line);
				break;
			}
			log.debug("parts["+processedLines+"; l="+parts.length+"]: "+Arrays.asList(parts));
			if(processedLines==0) {
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
				log.info("insert sql: "+sb.toString());
				stmt = conn.prepareStatement(sb.toString());
				//stmtStrPrep = sb.toString();
			}
			if(is1stloop && skipHeaderN>processedLines) {
				processedLines++;
				continue;
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
			processedLines++;
			importedLines += changedRows;
			if(commitEachXrows!=null && commitEachXrows>0 && (importedLines%commitEachXrows==0)) {
				doCommit(conn);
			}
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
