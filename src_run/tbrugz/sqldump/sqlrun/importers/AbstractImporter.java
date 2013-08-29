package tbrugz.sqldump.sqlrun.importers;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
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
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.NonNullGetMap;

public abstract class AbstractImporter extends AbstractFailable implements Executor {
	
	public static class IOCounter {
		long input = 0;
		long output = 0;

		void add(IOCounter other) {
			input += other.input;
			output += other.output;
		}
		
		@Override
		public String toString() {
			return "IOCounter[i="+input+";o="+output+"]";
		}
	}
	
	public enum FailoverIdSelectionStrategy {
		CYCLE,
		RESTART
	}
	
	//XXX: different exec suffixes for each importer class?
	static final String[] EXEC_SUFFIXES = {
		Constants.SUFFIX_IMPORT,
	};

	static final Log log = LogFactory.getLog(AbstractImporter.class);

	Properties prop;
	Connection conn;
	CommitStrategy commitStrategy;
	
	String execId = null;
	String importFile = null;
	String importDir = null;
	String importFiles = null;
	String importURL = null;
	String urlData = null;
	String urlMethod = null;
	boolean follow = false;
	String recordDelimiter = "\r?\n";
	String insertTable = null;
	String insertSQL = null;
	String defaultInputEncoding = DataDumpUtils.CHARSET_UTF8;
	String inputEncoding = defaultInputEncoding;
	List<String> columnTypes;
	
	boolean logMalformedLine = true;
	
	int maxFailoverId = 0;
	FailoverIdSelectionStrategy failoverStrategy = FailoverIdSelectionStrategy.CYCLE; //TODO: property for selecting failover strategy
	
	boolean useBatchUpdate = false;
	long batchUpdateSize = 1000;

	long commitEachXrows = 0l;
	static long defaultCommitEachXrowsForFileStrategy = 1000l;
	
	long sleepMilis = 100; //XXX: prop for sleepMilis (used in follow mode)?
	long skipHeaderN = 0;
	long logEachXrows = 10000l; //XXX: prop for logEachXrows

	//needed as a property for 'follow' mode
	InputStream fileIS = null;
	PreparedStatement stmt = null;

	//prefix
	public static final String PREFIX_FAILOVER = ".failover.";

	//suffixes
	static final String SUFFIX_IMPORTFILE = ".importfile";
	static final String SUFFIX_IMPORTDIR = ".importdir";
	static final String SUFFIX_IMPORTFILES = ".importfiles";
	static final String SUFFIX_IMPORTURL = ".importurl";
	static final String SUFFIX_URLMESSAGEBODY = ".urlmessagebody";
	static final String SUFFIX_URLMETHOD = ".urlmethod";
	
	static final String SUFFIX_FOLLOW = ".follow";
	static final String SUFFIX_RECORDDELIMITER = ".recorddelimiter";
	static final String SUFFIX_ENCLOSING = ".enclosing";
	static final String SUFFIX_INSERTTABLE = ".inserttable";
	static final String SUFFIX_INSERTSQL = ".insertsql";
	static final String SUFFIX_SKIP_N = ".skipnlines";
	static final String SUFFIX_COLUMN_TYPES = ".columntypes";
	
	static final String SUFFIX_LOG_MALFORMED_LINE = ".logmalformedline";
	static final String SUFFIX_X_COMMIT_EACH_X_ROWS = ".x-commiteachxrows"; //XXX: to be overrided by SQLRun (CommitStrategy: STATEMENT, ...)?
	
	static final String[] AUX_SUFFIXES = {
		SUFFIX_ENCLOSING,
		Constants.SUFFIX_ENCODING,
		SUFFIX_FOLLOW,
		SUFFIX_IMPORTFILE,
		SUFFIX_IMPORTDIR,
		SUFFIX_IMPORTFILES,
		SUFFIX_IMPORTURL,
		SUFFIX_INSERTTABLE,
		SUFFIX_INSERTSQL,
		SUFFIX_RECORDDELIMITER,
		SUFFIX_SKIP_N,
		SUFFIX_LOG_MALFORMED_LINE,
		SUFFIX_X_COMMIT_EACH_X_ROWS,
		SUFFIX_COLUMN_TYPES
	};
	
	@Override
	public void setExecId(String execId) {
		this.execId = execId;
	}
	
	@Override
	public void setProperties(Properties prop) {
		this.prop = prop;
		importFile = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_IMPORTFILE);
		importDir = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_IMPORTDIR);
		importFiles = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_IMPORTFILES);
		importURL = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_IMPORTURL);
		urlData = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_URLMESSAGEBODY);
		urlMethod = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_URLMETHOD);
		inputEncoding = prop.getProperty(Constants.PREFIX_EXEC+execId+Constants.SUFFIX_ENCODING, defaultInputEncoding);
		recordDelimiter = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_RECORDDELIMITER, recordDelimiter);
		skipHeaderN = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+SUFFIX_SKIP_N, skipHeaderN);
		follow = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_FOLLOW, follow);
		useBatchUpdate = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_BATCH_MODE, useBatchUpdate);
		batchUpdateSize = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_BATCH_SIZE, batchUpdateSize);
		
		long defaultCommitEachXrows = commitStrategy==CommitStrategy.FILE?defaultCommitEachXrowsForFileStrategy:commitEachXrows;
		commitEachXrows = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+SUFFIX_X_COMMIT_EACH_X_ROWS, defaultCommitEachXrows);
		
		logMalformedLine = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_LOG_MALFORMED_LINE, logMalformedLine);
		
		if(useBatchUpdate && commitEachXrows>0 && (commitEachXrows%batchUpdateSize)!=0) {
			log.warn("[execId="+execId+"] better if commit size ("+commitEachXrows+") is a multiple of batch size ("+batchUpdateSize+")...");
		}
		
		//set max failover id!
		maxFailoverId = getMaxFailoverId();
		
		/*columnTypes = Utils.getStringListFromProp(prop, Constants.PREFIX_EXEC+execId+SUFFIX_COLUMN_TYPES, ",");
		if(columnTypes!=null) {
			log.info("[execId="+execId+"] column-types: "+columnTypes);
		}*/
		
		setImporterProperties(prop);
		//setDefaultImporterProperties(prop);
	}

	void setImporterProperties(Properties prop, String importerPrefix) {
		insertTable = prop.getProperty(importerPrefix+SUFFIX_INSERTTABLE);
		insertSQL = prop.getProperty(importerPrefix+SUFFIX_INSERTSQL);
		columnTypes = Utils.getStringListFromProp(prop, importerPrefix+SUFFIX_COLUMN_TYPES, ",");
		mustSetupSQLStatement = true;
	}
	
	void setImporterProperties(Properties prop) {
		if(failoverId==0) {
			setImporterProperties(prop, Constants.PREFIX_EXEC+execId);
		}
		else {
			String failoverKey = Constants.PREFIX_EXEC+execId+PREFIX_FAILOVER+failoverId;
			setImporterProperties(prop, failoverKey);
		}
	}

	@Override
	public void setConnection(Connection conn) {
		this.conn = conn;
	}
	
	@Override
	public void setCommitStrategy(CommitStrategy commitStrategy) {
		this.commitStrategy = commitStrategy;
	}
	
	@Override
	public List<String> getAuxSuffixes() {
		List<String> ret = new ArrayList<String>();
		ret.addAll(Arrays.asList(AUX_SUFFIXES));
		return ret;
	}

	//XXX add countsByFailoverId for all files?
	Map<Integer, IOCounter> aggCountsByFailoverId;
	
	//XXX @Override
	public long importData() throws SQLException, InterruptedException, IOException {
		aggCountsByFailoverId = new NonNullGetMap<Integer, IOCounter>(new HashMap<Integer, IOCounter>(), IOCounter.class);
		long ret = 0;
		long filesImported = 0;
		if(commitEachXrows>0 && commitStrategy!=CommitStrategy.FILE) {
			log.warn("property '"+SUFFIX_X_COMMIT_EACH_X_ROWS+"' needs "+CommitStrategy.FILE+" commit strategy");
			commitEachXrows = 0;
		}
		String loginfo = (maxFailoverId>0?" [failoverstrategy="+failoverStrategy+"; maxfailoverid="+maxFailoverId+"]":"")+
				(commitEachXrows>0?" [commit-size="+commitEachXrows+"]":"")+
				(useBatchUpdate?" [batch-size="+batchUpdateSize+"]":"");
		
		try {
			
		if(importFile!=null) {
			log.info("importing file: "+importFile+loginfo);
			ret = importFile();
			filesImported++;
			addMapCount(aggCountsByFailoverId, countsByFailoverId);
		}
		else if(importFiles!=null) {
			if(importDir==null) {
				importDir = System.getProperty("user.dir");
			}
			log.info("importing files from dir: "+importDir+loginfo);
			List<String> files = Util.getFiles(importDir, importFiles);
			if(files==null || files.size()==0) {
				log.warn("no files in dir '"+importDir+"'...");
			}
			else {
				for(String file: files) {
					importFile = file;
					log.debug("importing file: "+importFile+loginfo);
					ret += importFile();
					filesImported++;
					addMapCount(aggCountsByFailoverId, countsByFailoverId);
				}
			}
		}
		else if(importURL!=null) {
			log.info("importing URL: "+importURL+loginfo);
			ret = importFile();
			filesImported++;
			addMapCount(aggCountsByFailoverId, countsByFailoverId);
		}
		else {
			log.error("neither '"+SUFFIX_IMPORTFILE+"', '"+SUFFIX_IMPORTFILES+"' nor '"+SUFFIX_IMPORTURL+"' suffix specified...");
			if(failonerror) { throw new ProcessingException("neither '"+SUFFIX_IMPORTFILE+"', '"+SUFFIX_IMPORTFILES+"' nor '"+SUFFIX_IMPORTURL+"' suffix specified..."); }
		}
		
		} catch(SQLException e) {
			log.warn("sqlexception: "+e);
			SQLUtils.xtraLogSQLException(e, log);
			throw e;
		}
		
		if(filesImported>1) {
			log.info("imported lines by failover id - all files [all="+ret+"]:");//"imported lines = "+ret);
			logCounts(aggCountsByFailoverId, true);
		}
		
		return ret;
	}
	
	void addMapCount(Map<Integer, IOCounter> agg, Map<Integer, IOCounter> cc) {
		for(Entry<Integer, IOCounter> entry: cc.entrySet()) {
			agg.get(entry.getKey()).add(entry.getValue());
		}
	}

	Map<Integer, IOCounter> countsByFailoverId;
	List<Integer> filecol2tabcolMap = null;
	boolean mustSetupSQLStatement = false;
	int failoverId = 0;
	
	long importFile() throws SQLException, InterruptedException, IOException {
		//init counters
		countsByFailoverId = new NonNullGetMap<Integer, IOCounter>(new HashMap<Integer, IOCounter>(), IOCounter.class);
		lastOutputCountCommit = 0;
		lastOutputCountLog = 0;
		
		//failoverId = 0;
		IOCounter counter = countsByFailoverId.get(failoverId);
		
		Scanner scan = createScanner();
		
		Pattern p = scan.delimiter();
		log.debug("scan delimiter pattern: "+p);
		//log.info("input file: "+importFile);
		
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
			int linecounter = 0;
			while(scan.hasNext()) {
				boolean importthisline = true;

				//select strategy
				if(failoverStrategy==FailoverIdSelectionStrategy.RESTART) {
					if(failoverId > 0) {
						failoverId = 0;
						counter = countsByFailoverId.get(failoverId);
						setImporterProperties(prop);
					}
				}
				int loopStartedWithFailoverId = failoverId;
				
				String line = scan.next();
				linecounter++;
				
				while(importthisline) {
					try {
						procLineInternal(line, is1stloop);
						importthisline = false;
						//log.debug("procline-ok ["+linecounter+"]: failid="+failoverId);
					}
					catch(Exception e) {
						counter.input++;
						//next failover id
						failoverId++;
						if(failoverId > maxFailoverId) {
							failoverId = 0;
						}
						counter = countsByFailoverId.get(failoverId);
						setImporterProperties(prop);
						
						//is last failover-id?
						if(failoverId == loopStartedWithFailoverId) {
							if(logMalformedLine) {
								log.warn("error processing line "+linecounter
										+(maxFailoverId>0?" ["+failoverId+"/"+maxFailoverId+"]: ":": ")
										+e);
								//log.debug("error processing line "+linecounter, e);
								//XXX: throw ProcessingException()?
							}
							importthisline = false;
							//break;
						}
					}
				} //while (importthisline)
				
			}
			
			cleanupStatement(counter);
			if(commitStrategy==CommitStrategy.FILE) {
				Util.doCommit(conn);
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
		
		if(fileIS!=null) { fileIS.close(); fileIS = null; }

		//show counters
		long countAll = logCounts(countsByFailoverId, false);
		
		return countAll;
	}
	
	long logCounts(Map<Integer, IOCounter> ccMap, boolean alwaysShowId) { // remove alwaysShowId?
		long countAllIn = 0, countAllOut = 0;
		int loopCount = 1, mapSize = ccMap.size();
		for(Entry<Integer, IOCounter> entry: ccMap.entrySet()) {
			IOCounter cc = entry.getValue();
			if(cc.input>0 || cc.output>0 || loopCount < mapSize) {
				log.info( ((mapSize > 1)?"[failover="+entry.getKey()+"] ":"") +"processedLines: "+cc.input+" ; importedRows: "+cc.output);
				countAllIn += cc.input;
				countAllOut += cc.output;
			}
			loopCount++;
		}
		if(mapSize > 1) {
			log.info("[failover=ALL] processedLines: "+countAllIn+" ; importedRows: "+countAllOut);
		}
		return countAllOut;
	}
	
	long lastOutputCountCommit = 0;
	long lastOutputCountLog = 0;
	
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
		
		//List<String> values = new ArrayList<String>();
		
		for(int i=0;i<parts.length;i++) {
			int index = i;
			try {
				
			if(filecol2tabcolMap!=null) {
				//log.info("v: "+i);
				if(filecol2tabcolMap.contains(i)) {
					index = filecol2tabcolMap.indexOf(i);
					stmtSetValue(index, parts[i]);
					//values.add(parts[i]);
					//log.info("v: "+i+" / "+index+"~"+(index+1)+" / "+parts[index]+" // "+parts[i]);						
				}
				else {
					//do nothing!
				}
			}
			else {
				stmtSetValue(index, parts[i]);
				//values.add(parts[i]);
			}
			
			}
			catch(RuntimeException e) {
				log.debug("error procLineInternal: i="+i+"; index="+index+" ; value='"+parts[i]+"'"
						+(columnTypes!=null?" ; type="+columnTypes.get(index):"")
						+": "+e);
				throw e;
			}
			//stmtStr = stmtStrPrep.replaceFirst("\\?", parts[i]);
		}
		
		//log.info("insert-values: "+values);
		
		//log.info("insert["+processedLines+"/"+importedLines+"]: "+stmtStr);
		//stmt.addBatch(); //XXXdone: batch insert? yes!
		if(useBatchUpdate) {
			stmt.addBatch();
			counter.input++;
			if((counter.input % batchUpdateSize) == 0) {
				cleanupStatement(counter);
			}
		}
		else {
			int changedRows = stmt.executeUpdate();
			counter.input++;
			counter.output += changedRows;
		}

		if(commitEachXrows>0 && (counter.output>lastOutputCountCommit) && (counter.output%commitEachXrows==0)) {
			//XXX commit size should be multiple of batch size?
			Util.doCommit(conn);
			lastOutputCountCommit = counter.output;
		}
		if(logEachXrows>0 && (counter.output>lastOutputCountLog) && (counter.output%logEachXrows==0)) {
			log.info("[exec-id="+execId+"] "+counter.output+" rows imported");
			lastOutputCountLog = counter.output;
		}
	}
	
	void stmtSetValue(int index, String value) throws SQLException {
		if(columnTypes!=null) {
			if(columnTypes.size()>index) {
				if(columnTypes.get(index).equals("int")) {
					stmt.setInt(index+1, Integer.parseInt(value.trim()));
					return;
				}
				//XXX: more column types (double, date, boolean, byte, long, object?, null?, ...)
			}
			else {
				log.warn("stmtSetValue: columnTypes.size() <= index [="+index+"] (will use 'string' type)");
			}
		}
		//default: set as string
		stmt.setString(index+1, value);
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
		
		if(stmt!=null) {
			cleanupStatement(countsByFailoverId.get(failoverId));
		}
		stmt = conn.prepareStatement(sb.toString());
		//stmtStrPrep = sb.toString();
	}
	
	void cleanupStatement(IOCounter counter) throws SQLException {
		if(stmt==null) {
			log.warn("null statement! in = "+counter.input);
			//new NullPointerException().printStackTrace();
			return;
		}
		
		if(useBatchUpdate) {
			int[] changedRowsArr = stmt.executeBatch();
			int sum = 0;
			for(int i=0;i<changedRowsArr.length;i++) {
				sum += changedRowsArr[i];
			}
			counter.output += sum;
			if(sum>0) {
				Util.logBatch.debug("cleanupStatement: executeBatch(): input = "+counter.input+" ; updates = "+changedRowsArr.length+" ; sum = "+sum);
			}
		}
	}
	
	int getMaxFailoverId() {
		for(int i=1;;i++) {
			String failoverKey = Constants.PREFIX_EXEC+execId+PREFIX_FAILOVER+i;
			List<String> foids = Utils.getKeysStartingWith(prop, failoverKey);
			if(foids==null || foids.size()==0) {
				return i-1;
			}
		}
	}

	abstract String[] procLine(String line, long processedLines) throws SQLException;
	
	static Map<String,String> cookiesHeader = new HashMap<String, String>();
	
	Scanner createScanner() throws MalformedURLException, IOException {
		Scanner scan = null;
		if(importURL!=null) {
			scan = new Scanner(getURLInputStream(importURL, urlMethod, urlData, cookiesHeader, 0), inputEncoding);
		}
		else if(Constants.STDIN.equals(importFile)) {
			scan = new Scanner(System.in, inputEncoding);
		}
		else {
			if(fileIS==null) {
				fileIS = new BufferedInputStream(new FileInputStream(importFile));
			}
			scan = new Scanner(fileIS, inputEncoding);
		}
		scan.useDelimiter(recordDelimiter);
		return scan;
	}
	
	static final int MAX_LEVEL = 5;
	
	static InputStream getURLInputStream(final String importURL, final String urlMethod, final String urlData, final Map<String,String> cookiesHeader, final int level)
			throws MalformedURLException, IOException {
		if(level>MAX_LEVEL) {
			throw new RuntimeException("max level redirection reached ("+MAX_LEVEL+")");
		}
		HttpURLConnection urlconn = (HttpURLConnection) new URL(importURL).openConnection();
		urlconn.setInstanceFollowRedirects(false);
		if(urlMethod!=null) {
			urlconn.setRequestMethod(urlMethod);
		}
		if(cookiesHeader!=null) {
			urlconn.setRequestProperty("Cookie", getCookieString(cookiesHeader));
		}
		if(urlData!=null) {
			log.info("urldata["+urlMethod+"]: "+urlData);
			urlconn.setDoOutput(true);
			urlconn.setFixedLengthStreamingMode(urlData.length());
			Writer w = new OutputStreamWriter( urlconn.getOutputStream() );
			w.write(urlData);
			w.flush();
		}
		urlconn.connect();
		//int responseCode = urlconn.getResponseCode();
		//log.debug("response-code: "+responseCode);
		String location = urlconn.getHeaderField("Location");
		if(location!=null) {
			log.info("location: "+location);
			
			List<String> cookies = urlconn.getHeaderFields().get("Set-Cookie");
			if(cookies!=null) {
				for(String c: cookies) {
					String[] cookie = c.split(";")[0].split("=");
					cookiesHeader.put(cookie[0], cookie[1]);
				}
			}
			
			if(!location.startsWith("http://") || !location.startsWith("https://")) {
				location = importURL.substring(0, importURL.lastIndexOf("/")+1) + location;
				log.debug("redir-location: "+location);
			}
			return getURLInputStream(location, urlMethod, urlData, cookiesHeader, level+1);
		}
		return urlconn.getInputStream();
	}
	
	static String getCookieString(final Map<String,String> cookiesHeader) {
		StringBuilder sb = new StringBuilder();
		boolean is1st = true;
		for(Entry<String,String> entry: cookiesHeader.entrySet()) {
			sb.append((is1st?"":"; ")+entry.getKey()+"="+entry.getValue());
			is1st = false;
		}
		return sb.toString();
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
