package tbrugz.sqldump.sqlrun.importers;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.CharBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
import tbrugz.sqldump.sqlrun.def.Importer;
import tbrugz.sqldump.sqlrun.def.Util;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.ShutdownManager;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.NonNullGetMap;

public abstract class AbstractImporter extends AbstractFailable implements Importer {
	
	public static class IOCounter {
		long input = 0;
		long output = 0;
		long successNoInfoCount = 0;
		long executeFailedCount = 0;

		void add(IOCounter other) {
			input += other.input;
			output += other.output;
		}
		
		@Override
		public String toString() {
			return "IOCounter[i="+input+";o="+output+"]";
		}
	}

	public static class ReadableStringList implements Readable {
		//XXX implements ReadableByteChannel?

		final List<String> buffer;
		final String delimiter;
		final int delimLen;
		int pos = 0;
		int strpos = 0;

		public ReadableStringList(List<String> buffer, String recordDelimiterReplacer) {
			this.buffer = buffer;
			this.delimiter = recordDelimiterReplacer!=null?recordDelimiterReplacer:"";
			this.delimLen = delimiter.length();
		}

		public int read(CharBuffer cb) {
			if(pos >= buffer.size()) {
				return -1;
			}
			String line = buffer.get(pos);
			String subline = strpos==0 ? line : line.substring(strpos);
			int cbl = cb.remaining();
			int read = 0;
			try {
				if(cbl<subline.length()+delimLen) {
					subline = subline.substring(0, cbl);
					//cb.clear();
					cb.put(subline);
					strpos += cbl;
					read = cbl;
					//log.info("[pos=="+pos+";read=="+read+";partial] subline = "+subline+" ; "+
					//	"position = "+cb.position()+" ; remaining = "+cb.remaining()); //+" ;charbuffer = "+cb);
				}
				else {
					//cb.clear();
					cb.put(subline);
					cb.append(delimiter);
					strpos = 0;
					read = subline.length();
					//log.info("[pos=="+pos+";read=="+read+"] subline = "+subline+" ; "+
					//	"position = "+cb.position()+" ; remaining = "+cb.remaining()); //+" ;charbuffer = "+cb);
					pos++;
				}
				return read;
			}
			catch(RuntimeException e) {
				log.warn("Exception: "+e+" [CharBuffer, class = "+cb.getClass().getName()+" , length = "+cb.length()+", line.length = "+line.length()+"] - line: "+line);
				throw e;
				//return -1;
			}
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
	static final Log logRow = LogFactory.getLog(AbstractImporter.class.getName()+"-row");
	
	static final int ERRORLINE_MAXSIZE = 40;
	static final long LOG_EACH_X_ROWS_DEFAULT = 10000L; //50000 ? (DataDump's default)

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
	Map<String,String> urlHeaders;
	
	boolean follow = false;
	String recordDelimiter = "\r?\n";
	String insertTable = null;
	String insertSQL = null;
	String defaultInputEncoding = DataDumpUtils.CHARSET_UTF8;
	String inputEncoding = defaultInputEncoding;
	List<String> columnTypes;
	Integer columnCount;
	Integer onErrorIntValue;
	
	boolean stmtSetNull4MissingCols = true;
	
	boolean logMalformedLine = true;
	
	int maxFailoverId = 0;
	FailoverIdSelectionStrategy failoverStrategy = FailoverIdSelectionStrategy.CYCLE; //TODO: property for selecting failover strategy
	
	boolean useBatchUpdate = false;
	long batchUpdateSize = 1000;
	boolean retryWithBatchOff = false;

	long commitEachXrows = 0L;
	static long defaultCommitEachXrowsForFileStrategy = 1000L;
	
	long sleepMilis = 100; //XXX: prop for sleepMilis (used in follow mode)?
	long skipHeaderN = 0;
	Pattern skipLineRegex = null;
	long maxLines = -1;
	long logEachXrows = 0; //10000L; //XXX: prop for logEachXrows
	long logEachXInputRows = LOG_EACH_X_ROWS_DEFAULT; //XXX: prop for logEachXInputRows

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
	static final String SUFFIX_URLHEADER = ".urlheader@";
	
	static final String SUFFIX_FOLLOW = ".follow";
	static final String SUFFIX_RECORDDELIMITER = ".recorddelimiter";
	static final String SUFFIX_ENCLOSING = ".enclosing";
	static final String SUFFIX_INSERTTABLE = ".inserttable";
	static final String SUFFIX_INSERTSQL = ".insertsql";
	static final String SUFFIX_SKIP_N = ".skipnlines";
	static final String SUFFIX_LIMIT_LINES = ".limit";
	static final String SUFFIX_SKIP_REGEX = ".skip-line-regex";
	static final String SUFFIX_COLUMN_TYPES = ".columntypes";
	static final String SUFFIX_ONERROR_TYPE_INT_SET_VALUE = ".onerror.type-int-value";
	//XXX: add '.onerror.type-(double|date)-value' ?
	
	static final String SUFFIX_LOG_MALFORMED_LINE = ".logmalformedline";
	static final String SUFFIX_X_COMMIT_EACH_X_ROWS = ".x-commiteachxrows"; //XXX: to be overrided by SQLRun (CommitStrategy: STATEMENT, ...)?
	
	static final String[] AUX_SUFFIXES = {
		SUFFIX_COLUMN_TYPES,
		SUFFIX_ENCLOSING,
		Constants.SUFFIX_ENCODING,
		SUFFIX_FOLLOW,
		SUFFIX_IMPORTFILE,
		SUFFIX_IMPORTDIR,
		SUFFIX_IMPORTFILES,
		SUFFIX_IMPORTURL,
		SUFFIX_INSERTTABLE,
		SUFFIX_INSERTSQL,
		SUFFIX_LOG_MALFORMED_LINE,
		SUFFIX_ONERROR_TYPE_INT_SET_VALUE,
		SUFFIX_RECORDDELIMITER,
		SUFFIX_SKIP_N,
		SUFFIX_SKIP_REGEX,
		SUFFIX_URLMESSAGEBODY,
		SUFFIX_URLMETHOD,
		SUFFIX_X_COMMIT_EACH_X_ROWS
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
		maxLines = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+SUFFIX_LIMIT_LINES, maxLines);
		String skipLineRegexStr = prop.getProperty(Constants.PREFIX_EXEC+execId+SUFFIX_SKIP_REGEX);
		if(skipLineRegexStr!=null) {
			skipLineRegex = Pattern.compile(skipLineRegexStr);
		}
		follow = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+SUFFIX_FOLLOW, follow);
		useBatchUpdate = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_BATCH_MODE, useBatchUpdate);
		batchUpdateSize = Utils.getPropLong(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_BATCH_SIZE, batchUpdateSize);
		retryWithBatchOff = Utils.getPropBool(prop, Constants.PREFIX_EXEC+execId+Constants.SUFFIX_BATCH_RETRY_OFF, retryWithBatchOff);
		onErrorIntValue = Utils.getPropInt(prop, Constants.PREFIX_EXEC+execId+SUFFIX_ONERROR_TYPE_INT_SET_VALUE);
		String urlHeaderPrefix = Constants.PREFIX_EXEC+execId+SUFFIX_URLHEADER;
		List<String> headerKeys = Utils.getKeysStartingWith(prop, urlHeaderPrefix);
		if(headerKeys!=null) {
			urlHeaders = new HashMap<String, String>();
			for(String key: headerKeys) {
				urlHeaders.put(key.substring(urlHeaderPrefix.length()), prop.getProperty(key));
			}
		}
		else {
			urlHeaders = null;
		}
		
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
		if(columnTypes!=null) {
			columnCount = columnTypes.size();
		}
		//XXX add prop for columnCount?
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

	List<String> batchRetryBuffer = null;
	
	@Override
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
					log.info("importing file: "+importFile+loginfo);
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
	boolean colTypesIndexFromTabCol = true;
	boolean mustSetupSQLStatement = false;
	int failoverId = 0;
	
	@SuppressWarnings("resource")
	long importFile() throws SQLException, InterruptedException, IOException {
		//init counters
		countsByFailoverId = new NonNullGetMap<Integer, IOCounter>(new HashMap<Integer, IOCounter>(), IOCounter.class);
		lastOutputCountCommit = 0;
		lastInputCountLog = 0;
		lastOutputCountLog = 0;
		
		//failoverId = 0;
		IOCounter counter = countsByFailoverId.get(failoverId);
		
		Scanner scan = createScanner();
		
		Pattern p = scan.delimiter();
		log.debug("scan delimiter pattern: ["+p+"]");
		log.debug("columnTypes"+(columnCount!=null?"[#"+columnCount+"]":"")+": "+columnTypes);
		//log.info("input file: "+importFile);

		if(useBatchUpdate && retryWithBatchOff) {
			batchRetryBuffer = new ArrayList<String>();
		}
		RuntimeException batchException = null;
		
		if(follow) {
			//add shutdown hook
			log.info("adding shutdown hook...");
			ShutdownManager.instance().removeAllHooks();
			ShutdownManager.instance().addShutdownHook(getShutdownThread());
		}
		
		boolean is1stloop = true;
		//int[] filecol2tabcolMap = null;
		do {
			int linecounter = 0;
			scanNext:
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
					if(maxLines >= 0 && counter.input > maxLines) {
						logRow.info("max (limit) rows reached: "+maxLines+" [linecounter="+linecounter+"]"); 
						break scanNext;
					}
					try {
						String[] lineParts = procLineInternal(line, is1stloop);
						boolean isLineComplete = isLastLineComplete();
						String finalLine = line;

						if(!isLineComplete) {
							StringBuilder sb = new StringBuilder();
							sb.append(line);
							while(!isLineComplete) {
								line = scan.next();
								linecounter++;
								if(batchException!=null) {
									//log.info("[retryWithBatchOff] line read from buffer [2] = "+line);
								}
								String rdr = recordDelimiterReplacer();
								if(rdr!=null) {
									sb.append(rdr); // should be scan.lastDelimiter?!?
								}
								sb.append(line);
								lineParts = procLineInternal(sb.toString(), is1stloop);
								isLineComplete = isLastLineComplete();
							}
							finalLine = sb.toString();
						}

						if(batchRetryBuffer!=null) {
							if(batchException==null && lineParts!=null) {
								batchRetryBuffer.add(finalLine);
								if(batchRetryBuffer.size()>batchUpdateSize) {
									batchRetryBuffer.remove(0);
								}
							}
							else {
								//log.info("[retryWithBatchOff] line read from buffer = "+line);
							}
						}

						if(lineParts!=null) {
							persistLineParts(lineParts);
						}
						importthisline = false;
						//log.debug("procline-ok ["+linecounter+"]: failid="+failoverId);
					}
					catch(Exception e) {
						counter.input++;

						// batch-retry
						if(e instanceof SQLException && useBatchUpdate && retryWithBatchOff) {
							batchException = new RuntimeException("[retryWithBatchOff] exception on line #"+counter.input+
								", will rerun with batch off from line #"+(counter.input - batchUpdateSize), e);
							log.warn("[retryWithBatchOff] row number [counter.input = "+counter.input+" ; linecounter = "+linecounter+"] will rewind "+batchUpdateSize+" lines & turn off batch mode ; "+
								"SQLException: "+e.toString().trim());
							counter.input -= batchUpdateSize;
							linecounter -= batchUpdateSize;
							useBatchUpdate = false;
							scan = createScannerFromBuffer(batchRetryBuffer);
							//log.info("[retryWithBatchOff] batchRetryBuffer.size()=="+batchRetryBuffer.size());
						}
						else {

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
								logRow.warn("error processing line "+linecounter
										+(maxFailoverId>0?" ["+failoverId+"/"+maxFailoverId+"]: ":": ")
										+e.toString().trim());
								if(batchException!=null) {
									//XXX warn [retryWithBatchOff]?
									logRow.info("[retryWithBatchOff] line #"+linecounter+": "+line);
								}
								//log.info("error processing line "+linecounter, e);
								//XXX: ??
								//if(failonerror) {
								//	throw new ProcessingException("Error processing line "+linecounter, e);
								//}
							}
							importthisline = false;
							//break;
						}

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
				//log.debug("available: "+fileIS.available());
				scan = createScanner(); 
			}
			is1stloop = false;
		}
		while(follow);
		
		if(fileIS!=null) { fileIS.close(); fileIS = null; }
		if(scan!=null) { scan.close(); }

		//show counters
		long countAll = logCounts(countsByFailoverId, false);

		if(batchException!=null) {
			throw batchException;
		}

		return countAll;
	}
	
	Thread getShutdownThread() {
		return new Thread() {
			public void run() {
				log.info("[shutdown] commiting & shutting down...");
				System.err.println("[shutdown] commiting & shutting down...");
				try {
					conn.commit();
				} catch (SQLException e) {
					log.warn("[shutdown] error commiting: "+e);
					System.err.println("[shutdown] error commiting: "+e);
				}
				log.info("[shutdown] shutting down");
				System.err.println("[shutdown] shutting down");
			}
		};		
	}
	
	long logCounts(Map<Integer, IOCounter> ccMap, boolean alwaysShowId) { // remove alwaysShowId?
		long countAllIn = 0, countAllOut = 0;
		long countAllSNI = 0, countAllEF = 0;
		int loopCount = 1, mapSize = ccMap.size();
		for(Entry<Integer, IOCounter> entry: ccMap.entrySet()) {
			IOCounter cc = entry.getValue();
			if(cc.input>0 || cc.output>0 || loopCount < mapSize) {
				log.info( ((mapSize > 1)?"[failover="+entry.getKey()+"] ":"") +"processedLines: "+cc.input+" ; importedRows: "+cc.output
					+( (cc.successNoInfoCount>0||cc.executeFailedCount>0)?" [successNoInfoCount=="+cc.successNoInfoCount+" ; executeFailedCount=="+cc.executeFailedCount+"]":"")
					);
				countAllIn += cc.input;
				countAllOut += cc.output;
				countAllSNI += cc.successNoInfoCount;
				countAllEF += cc.executeFailedCount;
			}
			loopCount++;
		}
		if(mapSize > 1) {
			log.info("[failover=ALL] processedLines: "+countAllIn+" ; importedRows: "+countAllOut
				+( (countAllSNI>0||countAllEF>0)?" [successNoInfoCount=="+countAllSNI+" ; executeFailedCount=="+countAllEF+"]":"")
			);
		}
		return countAllOut;
	}
	
	long lastOutputCountCommit = 0;
	long lastInputCountLog = 0;
	long lastOutputCountLog = 0;
	
	/**
	 * @return true if parser should proceed to next line, false otherwise
	 */
	String[] procLineInternal(String line, boolean is1stloop) throws SQLException {
		//log.info("line["+processedLines+"]: "+line);
		IOCounter counter = countsByFailoverId.get(failoverId);
		String[] parts = procLine(line, counter.input);
		if(!isLastLineComplete()) { return null; }
		
		if(parts==null) {
			String lineTrunc = strTruncated(line, ERRORLINE_MAXSIZE);
			log.debug("line could not be processed: "+lineTrunc);
			throw new RuntimeException("line could not be processed: "+lineTrunc);
		}
		
		if(log.isDebugEnabled()) {
			log.debug("parts[count="+counter.input+"; parts="+parts.length+"]: "+Arrays.asList(parts)+" ; columnTypes="+columnTypes);
		}
		
		if(counter.input==0 || mustSetupSQLStatement ) {
			setupSQLStatement(parts);
			mustSetupSQLStatement = false;
		}
		if(is1stloop && skipHeaderN>counter.input) {
			counter.input++;
			logNRows(counter);
			return null;
		}
		if(skipLineRegex!=null && skipLineRegex.matcher(line).find()) {
			counter.input++;
			logNRows(counter);
			return null;
		}
		
		if(columnCount!=null && parts.length < columnCount) {
			log.debug("#parts ["+parts.length+"] < #columnTypes/columnCount ["+columnCount+"] - set null for missing cols? "+stmtSetNull4MissingCols);
		}

		return parts;
	}

	boolean persistLineParts(String[] parts) throws SQLException {
		List<Integer> partsNotFound = new ArrayList<Integer>();
		int countNFE = 0, countPE = 0;
		IOCounter counter = countsByFailoverId.get(failoverId);

		//TODO: what if parts.length for some lines is shorter than others? stmt will keep old value from longer line...
		for(int i=0;i<parts.length;i++) {
			int index = i;
			try {
				
			if(filecol2tabcolMap!=null && filecol2tabcolMap.size()>0) {
				//log.info("v: "+i);
				if(filecol2tabcolMap.contains(i)) {
					index = filecol2tabcolMap.indexOf(i);
					int colIndex = colTypesIndexFromTabCol?index:i;
					//log.debug("v0: "+i+" / "+index+"~"+(index+1)+" / "+parts[i]+" // "+ (columnTypes.size()>colIndex?columnTypes.get(colIndex):"-") );
					stmtSetValue(index, parts[i], colIndex);
					//values.add(parts[i]);
					//log.info("v1: "+i+" / "+index+"~"+(index+1)+" / "+parts[index]+" // "+parts[i]+" // "+ (columnTypes.size()>colIndex?columnTypes.get(colIndex):"-") );
				}
				else {
					//do nothing!
					partsNotFound.add(i);
				}
			}
			else {
				stmtSetValue(index, parts[i], i);
				//values.add(parts[i]);
			}
			
			}
			catch(NumberFormatException nfe) {
				countNFE++;
				log.debug("nfe: "+nfe+" i=="+i);
				stmtSetNull(index);
			}
			catch(ParseException pe) {
				countPE++;
				log.debug("pe: "+pe+" i=="+i);
				stmtSetNull(index);
			}
			catch(RuntimeException e) {
				log.debug("error procLineInternal: i="+i+"; index="+index+" ; value='"+parts[i]+"'"
						+(columnTypes!=null?" ; type="+columnTypes.get(index):"")
						+": "+e);
				throw e;
			}
			//stmtStr = stmtStrPrep.replaceFirst("\\?", parts[i]);
		}
		if(stmtSetNull4MissingCols && columnCount!=null && parts.length < columnCount) {
			for(int i=parts.length;i<columnCount;i++) {
				//log.debug("setNull "+(i+1));
				stmtSetNull(i);
			}
		}
		
		if(partsNotFound.size()>0) {
			log.debug("filecol2tabcolMap does not contain parts "+partsNotFound);
		}
		
		//log.info("insert-values: "+values);
		
		//log.info("insert["+processedLines+"/"+importedLines+"]: "+stmtStr);
		//stmt.addBatch(); //XXXdone: batch insert? yes!
		if(useBatchUpdate) {
			try {
				stmt.addBatch();
				counter.input++;
				if((counter.input % batchUpdateSize) == 0) {
					cleanupStatement(counter);
				}
			}
			catch(SQLException e) {
				//XXX logging may show posterior line
				//log.debug("sql-exception(batch): counter.input="+counter.input+" ; parts="+Arrays.toString(parts));
				throw e;
			}
		}
		else {
			try {
				int changedRows = stmt.executeUpdate();
				counter.input++;
				counter.output += changedRows;
			}
			catch(SQLException e) {
				log.debug("SQLException[counter.input="+counter.input+" ; parts="+Arrays.toString(parts)+"]: "+e.getMessage());
				throw e;
			}
		}
		
		if(countNFE>0 || countPE>0) {
			log.debug("countNFE = "+countNFE+" ; countPE = "+countPE);
		}

		if(commitEachXrows>0 && (counter.output>lastOutputCountCommit) && (counter.output%commitEachXrows==0)) {
			//XXX commit size should be multiple of batch size?
			Util.doCommit(conn);
			lastOutputCountCommit = counter.output;
		}
		logNRows(counter);
		return true;
	}

	void logNRows(IOCounter counter) {
		// log input
		if(logEachXInputRows>0 && (counter.input>lastInputCountLog) && (counter.input%logEachXInputRows==0)) {
			logRow.info("[exec-id="+execId+"] "+counter.input+" rows read ["+counter.output+" rows imported]"
				+( (counter.successNoInfoCount>0||counter.executeFailedCount>0)?" [successNoInfoCount=="+counter.successNoInfoCount+" ; executeFailedCount=="+counter.executeFailedCount+"]":"")
				);
			lastInputCountLog = counter.input;
		}
		// log output
		if(logEachXrows>0 && (counter.output>lastOutputCountLog) && (counter.output%logEachXrows==0)) {
			logRow.info("[exec-id="+execId+"] "+counter.output+" rows imported");
			lastOutputCountLog = counter.output;
		}
	}
	
	String strTruncated(String s, int max) {
		if(s.length()>max) {
			s = s.substring(0, max)+"...";
		}
		return s.replaceAll("\\n", " ");
	}

	void stmtSetValue(int index, String value, int colTypeIndex) throws SQLException, ParseException {
		if(Utils.isNullOrEmpty(value)) {
			stmtSetNull(index);
			return;
		}
		if(columnTypes!=null) {
			if(columnTypes.size()>colTypeIndex) {
				String colType = columnTypes.get(colTypeIndex);
				//log.info("i: "+index+" ; type: "+colType+" ; value: '"+value+"'");
				
				if(colType.equals("int")) {
					try {
						//log.info("int:: "+(index+1)+" / "+value);
						stmt.setInt(index+1, Integer.parseInt(value.trim()));
					}
					catch(NumberFormatException e) {
						if(onErrorIntValue!=null) {
							stmt.setInt(index+1, onErrorIntValue);
						}
						else {
							throw e;
						}
					}
				}
				else if(colType.equals("double")) {
					stmt.setDouble(index+1, Double.parseDouble(value.replaceAll(",", "").trim()));
				}
				else if(colType.equals("doublec")) {
					stmt.setDouble(index+1, Double.parseDouble(value.replaceAll("\\.", "").replaceAll(",", ".").trim()));
				}
				else if(colType.equals("string")) {
					stmt.setString(index+1, value);
				}
				else if(colType.equals("object")) {
					stmt.setObject(index+1, value);
				}
				else if(colType.equals("null")) {
					//stmt.setObject(index+1, null);
					stmtSetNull(index);
				}
				else if(colType.startsWith("date[")) {
					//XXX use java.sql.Timestamp?
					//XXX setup DateFormat only once for each column?
					String strFormat = colType.substring(5, colType.indexOf(']'));
					DateFormat df = new SimpleDateFormat(strFormat);
					try {
						java.sql.Date date = new java.sql.Date( df.parse(value).getTime() );
						stmt.setDate(index+1, date);
						//log.info("date:: "+(index+1)+" / "+value+" / "+date);
					}
					catch(ParseException e) {
						log.warn("parse exception:: idx="+(index+1)+" ; value="+value+" ; format="+strFormat);
						throw e;
					}
				}
				else if(colType.equals("blob-location") || colType.equals("text-location")) {
					File f = new File(value);
					if(!f.exists()) {
						log.warn("file '"+f+"' not found [col# = "+(index+1)+"]");
					}
					try {
						if(colType.equals("blob-location")) {
							//stmt.setBinaryStream(index+1, new FileInputStream(f));
							stmt.setBlob(index+1, new FileInputStream(f));
						}
						else if(colType.equals("text-location")) {
							//[inputstream]: asciistream ; [reader]: characterstream, clob, ncharacterstream, nclob
							stmt.setCharacterStream(index+1, new FileReader(f));
						}
						else {
							//XXX throw?
							log.warn("unknown colType: "+colType);
						}
					} catch (Exception e) {
						log.warn("Error importing '"+colType+"' file '"+f+"': "+e);
					}
				}
				else {
					//XXX throw?
					log.warn("stmtSetValue: unknown columnTypes '"+colType+"' [#"+index+"] (will use 'string' type)");
					stmt.setString(index+1, value);
				}
				//XXX: more column types (boolean, byte, long, object?, null?, ...)
				return;
			}
			else {
				log.warn("stmtSetValue: columnTypes.size() <= index [="+index+"] (will use 'string' type)");
			}
		}
		//default: set as string
		//log.debug("stmtSetValue: index [="+(index+1)+"]: "+value);
		stmt.setString(index+1, value);
	}

	void stmtSetNull(int index) throws SQLException {
		stmt.setObject(index+1, null);
		return;
	}
	
	List<Integer> loggedStatementFailoverIds = new ArrayList<Integer>();
	
	//TODO: map of statements (one for each failoverId)?
	void setupSQLStatement(String[] parts) throws SQLException {
		StringBuilder sb = new StringBuilder();
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
			int successNoInfoCount = 0;
			int executeFailedCount = 0;
			int unknownCount = 0;
			for(int i=0;i<changedRowsArr.length;i++) {
				int val = changedRowsArr[i];
				if(val > 0) {
					sum += val;
				}
				else if(val == 0) {}
				else if(val == Statement.SUCCESS_NO_INFO) {
					successNoInfoCount++;
				}
				else if(val == Statement.EXECUTE_FAILED) {
					executeFailedCount++;
				}
				else {
					unknownCount++;
				}
			}
			counter.output += sum;
			counter.successNoInfoCount += successNoInfoCount;
			counter.executeFailedCount += executeFailedCount;

			//if(sum>0) {
			Util.logBatch.debug("cleanupStatement: executeBatch(): input = "+counter.input+" ; updates = "+changedRowsArr.length+" ; sum = "+sum+
				( (successNoInfoCount>0||executeFailedCount>0||unknownCount>0)?" [successNoInfoCount=="+successNoInfoCount+" ; executeFailedCount=="+executeFailedCount+" ; unknownCount=="+unknownCount+"]":"")
				);
			//}
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

	abstract boolean isLastLineComplete();
	
	abstract String recordDelimiterReplacer();
	
	static Map<String,String> cookiesHeader = new HashMap<String, String>();

	Scanner createScanner() throws IOException {
		Scanner scan = null;
		if(importURL!=null) {
			scan = new Scanner(getURLInputStream(importURL, urlMethod, urlData, cookiesHeader, urlHeaders, 0), inputEncoding);
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

	Scanner createScannerFromBuffer(List<String> buffer) {
		Scanner scan = new Scanner(new ReadableStringList(buffer, recordDelimiterReplacer()));
		scan.useDelimiter(recordDelimiter);
		return scan;
	}

	static final int MAX_LEVEL = 5;
	
	static InputStream getURLInputStream(final String importURL, final String urlMethod, final String urlData, final Map<String,String> cookiesHeader, final Map<String,String> urlHeaders, final int level)
			throws IOException {
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
		if(urlHeaders!=null && urlHeaders.size()>0) {
			for(Entry<String, String> headerEntry: urlHeaders.entrySet()) {
				//log.debug("header:: "+headerEntry.getKey()+": "+headerEntry.getValue());
				urlconn.setRequestProperty(headerEntry.getKey(), headerEntry.getValue());
			}
			log.info("headers added: "+Utils.join(urlHeaders.keySet(), ", "));
		}
		if(urlData!=null) {
			log.info("urldata[method="+urlMethod+"]: "+urlData);
			urlconn.setDoOutput(true);
			urlconn.setFixedLengthStreamingMode(urlData.length());
			Writer w = new OutputStreamWriter( urlconn.getOutputStream() );
			w.write(urlData);
			w.flush();
		}
		urlconn.connect();
		int responseCode = urlconn.getResponseCode();
		//log.debug("response-code: "+responseCode);
		
		//responde-code >= 500 - error
		if(responseCode>=500) {
			return urlconn.getErrorStream();
		}
		
		//XXX response-code >= 400: getErrorStream?
		
		//responde-code >= 300 - redirect ?
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
			return getURLInputStream(location, urlMethod, urlData, cookiesHeader, urlHeaders, level+1);
		}
		
		//responde-code >= 200 - ok
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
