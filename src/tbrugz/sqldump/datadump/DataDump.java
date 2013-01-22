package tbrugz.sqldump.datadump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBIdentifiable;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;

/*
 * TODOne: prop for selecting which tables to dump data from
 * TODOne: limit number of rows to dump
 * TODOne: where clause for data dump 
 * TODOne: column values escaping
 * TODOne: 'insert into' datadump syntax:
 *   sqldump.datadump.useinsertintosyntax=false
 *   sqldump.datadump.useinsertintosyntax.withcolumnnames=true
 * XXXdone: refactoring: unify dumpDataRawSyntax & dumpDataInsertIntoSyntax
 * XXXxx: property for selecting which columns to dump
 * XXXdone: order-by-primary-key prop? asc, desc?
 * TODOne: dumpsyntaxes: x InsertInto, x CSV, xml, x JSON, x fixedcolumnsize
 * XXXdone: refactor: add abstract class OutputSyntax and XMLOutput, CSVOutput, ...
 * TODO: floatFormatter!
 * TODO: option to include, or not, partition columns in output
 */
public class DataDump extends AbstractSQLProc {

	//generic props
	public static final String PROP_DATADUMP_OUTFILEPATTERN = "sqldump.datadump.outfilepattern";
	//static final String PROP_DATADUMP_INSERTINTO = "sqldump.datadump.useinsertintosyntax";
	static final String PROP_DATADUMP_SYNTAXES = "sqldump.datadump.dumpsyntaxes";
	static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.charset";
	static final String PROP_DATADUMP_ROWLIMIT = "sqldump.datadump.rowlimit";
	static final String PROP_DATADUMP_TABLES = "sqldump.datadump.tables";
	static final String PROP_DATADUMP_IGNORETABLES = "sqldump.datadump.ignoretables";
	static final String PROP_DATADUMP_DATEFORMAT = "sqldump.datadump.dateformat";
	static final String PROP_DATADUMP_ORDERBYPK = "sqldump.datadump.orderbypk";
	static final String PROP_DATADUMP_TABLETYPES = "sqldump.datadump.tabletypes";
	static final String PROP_DATADUMP_LOG_EACH_X_ROWS = "sqldump.datadump.logeachxrows";
	static final String PROP_DATADUMP_LOG_1ST_ROW = "sqldump.datadump.log1strow";
	static final String PROP_DATADUMP_WRITEBOM = "sqldump.datadump.writebom";
	static final String PROP_DATADUMP_WRITEAPPEND = "sqldump.datadump.writeappend";

	//defaults
	static final String CHARSET_DEFAULT = DataDumpUtils.CHARSET_UTF8;
	static final long LOG_EACH_X_ROWS_DEFAULT = 50000;
	
	static final String FILENAME_PATTERN_TABLE_QUERY_ID = "\\$\\{id\\}";
	public static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	//static final String FILENAME_PATTERN_QUERYNAME = "\\$\\{queryname\\}";
	public static final String FILENAME_PATTERN_SYNTAXFILEEXT = "\\$\\{syntaxfileext\\}";
	
	static Log log = LogFactory.getLog(DataDump.class);
	static Log logDir = LogFactory.getLog(DataDump.class.getName()+".datadump-dir");
	static Log logRow = LogFactory.getLog(DataDump.class.getName()+".datadump-row");
	
	/*
	 * charset: http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html
	 *
	 * US-ASCII 	Seven-bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set
	 * ISO-8859-1   	ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
	 * UTF-8 	Eight-bit UCS Transformation Format
	 * UTF-16BE 	Sixteen-bit UCS Transformation Format, big-endian byte order
	 * UTF-16LE 	Sixteen-bit UCS Transformation Format, little-endian byte order
	 * UTF-16 	Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark
	 *
	 * XXX: use java.nio.charset.Charset.availableCharsets() ?
	 *  
	 */
	
	@Override
	public void process() {
		dumpData(conn, model.getTables(), prop);
	}

	//TODOne: filter tables by table type (table, view, ...)
	public void dumpData(Connection conn, Collection<Table> tablesForDataDump, Properties prop) {
		log.info("data dumping...");
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		
		String dateFormat = prop.getProperty(PROP_DATADUMP_DATEFORMAT);
		if(dateFormat!=null) {
			DataDumpUtils.dateFormatter = new SimpleDateFormat(dateFormat);
		}
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean orderByPK = Utils.getPropBool(prop, PROP_DATADUMP_ORDERBYPK, true);

		List<String> tables4dump = getTables4dump(prop);
		
		List<DumpSyntax> syntaxList = new ArrayList<DumpSyntax>();
		
		String syntaxes = prop.getProperty(PROP_DATADUMP_SYNTAXES);
		if(syntaxes==null) {
			log.warn("no datadump syntax defined");
			return;
		}
		String[] syntaxArr = syntaxes.split(",");
		for(String syntax: syntaxArr) {
			boolean syntaxAdded = false;
			for(Class<? extends DumpSyntax> dsc: DumpSyntax.getSyntaxes()) {
				DumpSyntax ds = (DumpSyntax) Utils.getClassInstance(dsc);
				if(ds!=null && ds.getSyntaxId().equals(syntax.trim())) {
					ds.procProperties(prop);
					syntaxList.add(ds);
					syntaxAdded = true;
				}
			}
			if(!syntaxAdded) {
				log.warn("unknown datadump syntax: "+syntax.trim());
			}
		}
		
		List<TableType> typesToDump = new ArrayList<TableType>();
		List<String> types = Utils.getStringListFromProp(prop, PROP_DATADUMP_TABLETYPES, ",");
		if(types!=null) {
			for(String type: types) {
				try {
					TableType ttype = TableType.valueOf(type.trim());
					typesToDump.add(ttype);
				}
				catch(IllegalArgumentException e) {
					log.warn("unknown table type: "+type.trim());
				}
			}
			log.info("table types for dumping: "+Utils.join(typesToDump, ", "));
		}
		else {
			typesToDump.addAll(Arrays.asList(TableType.values()));
			typesToDump.remove(TableType.VIEW);
			typesToDump.remove(TableType.MATERIALIZED_VIEW);
		}
		
		List<String> ignoretablesregex = Utils.getStringListFromProp(prop, PROP_DATADUMP_IGNORETABLES, "\\|");
		if(ignoretablesregex!=null) {
			for(int i=0;i<ignoretablesregex.size();i++) {
				ignoretablesregex.set(i, ignoretablesregex.get(i).trim());
			}
		}
		
		String quote = DBMSResources.instance().getIdentifierQuoteString();
		
		LABEL_TABLE:
		for(Table table: tablesForDataDump) {
			String tableName = table.getName();
			if(tables4dump!=null) {
				if(!tables4dump.contains(tableName)) { continue; }
				else { tables4dump.remove(tableName); }
			}
			if(typesToDump!=null) {
				if(!typesToDump.contains(table.getType())) { continue; }
			}
			if(ignoretablesregex!=null) {
				for(String tregex: ignoretablesregex) {
					if(tableName.matches(tregex)) {
						log.info("ignoring table: "+tableName);
						continue LABEL_TABLE;
					}
				}
			}
			List<FK> importedFKs = DBIdentifiable.getImportedKeys(table, model.getForeignKeys());
			List<Constraint> uniqueKeys = DBIdentifiable.getUKs(table);
			
			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.datadump."+tableName+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			String whereClause = prop.getProperty("sqldump.datadump."+tableName+".where");
			String selectColumns = prop.getProperty("sqldump.datadump."+tableName+".columns");
			if(selectColumns==null) { selectColumns = "*"; }
			String orderClause = prop.getProperty("sqldump.datadump."+tableName+".order");
			if(orderClause==null && orderByPK) { 
				Constraint ctt = table.getPKConstraint();
				if(ctt!=null) {
					orderClause = Utils.join(ctt.uniqueColumns, ", ", new StringDecorator.StringQuoterDecorator(quote));
				}
				else {
					log.warn("table '"+tableName+"' has no PK for datadump ordering");
				}
			}
			List<String> pkCols = null;  
			if(table.getPKConstraint()!=null) {
				pkCols = table.getPKConstraint().uniqueColumns;
			} 

			log.debug("dumping data/inserts from table: "+tableName);
			//String sql = "select "+selectColumns+" from \""+table.schemaName+"."+tableName+"\""
			
			String sql = "select "+selectColumns
					+" from "+(table.getSchemaName()!=null?table.getSchemaName()+".":"")+quote+tableName+quote
					+ (whereClause!=null?" where "+whereClause:"")
					+ (orderClause!=null?" order by "+orderClause:"");
			log.debug("sql: "+sql);
			
			try {
				//XXX: table dump with partitionBy?
				runQuery(conn, sql, null, prop, tableName, tableName, charset, 
						rowlimit,
						syntaxList,
						null,
						pkCols,
						importedFKs,
						uniqueKeys
						);
			}
			catch(Exception e) {
				log.warn("error dumping data from table: "+tableName+"\n\tsql: "+sql+"\n\texception: "+e);
				log.info("exception:", e);
			}
		}
		
		if(tables4dump!=null && tables4dump.size()>0) {
			log.warn("tables selected for dump but not found: "+Utils.join(tables4dump, ", "));
		}
	}
	
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop, 
			String tableOrQueryId, String tableOrQueryName, String charset,
			long rowlimit, List<DumpSyntax> syntaxList
			) throws Exception {
		runQuery(conn, sql, params, prop, tableOrQueryId, tableOrQueryName, charset, rowlimit, syntaxList, null, null, null, null);
	}
		
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop, 
			String tableOrQueryId, String tableOrQueryName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList,
			String[] partitionByPatterns,
			List<String> keyColumns,
			List<FK> importedFKs,
			List<Constraint> uniqueKeys
			) throws Exception {
		
			PreparedStatement st = conn.prepareStatement(sql);
			//st.setFetchSize(20);
			if(params!=null) {
				for(int i=0;i<params.size();i++) {
					st.setString(i+1, params.get(i));
				}
			}
			
			long initTime = System.currentTimeMillis();
			long dump1stRowTime = -1;
			logRow.info("[qid="+tableOrQueryId+"] running query '"+tableOrQueryName+"'");
			ResultSet rs = st.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			
			if(log.isDebugEnabled()) { //XXX: debug enabled? any better way?
				DataDumpUtils.logResultSetColumnsTypes(md, tableOrQueryName);
			}

			boolean hasData = rs.next();
			//so empty tables do not create empty dump files
			if(!hasData) {
				log.info("table/query '"+tableOrQueryName+"' returned 0 rows");
				return;
			}
			long count = 0;
			
			Map<String, Writer> writersOpened = new HashMap<String, Writer>();
			Map<String, DumpSyntax> writersSyntaxes = new HashMap<String, DumpSyntax>();
			
			if(partitionByPatterns==null) { partitionByPatterns = new String[]{ "" }; }
			
			List<String> filenameList = new ArrayList<String>();
			List<Boolean> doSyntaxDumpList = new ArrayList<Boolean>();
			
			String partitionByStrId = "";
			
			Boolean writeBOM = Utils.getPropBoolean(prop, PROP_DATADUMP_WRITEBOM, null);
			boolean writeAppend = Utils.getPropBoolean(prop, PROP_DATADUMP_WRITEAPPEND, false);
			
			boolean log1stRow = Utils.getPropBool(prop, PROP_DATADUMP_LOG_1ST_ROW, true);
			boolean logNumberOfOpenedWriters = true;
			long logEachXRows = Utils.getPropLong(prop, PROP_DATADUMP_LOG_EACH_X_ROWS, LOG_EACH_X_ROWS_DEFAULT);

			//header
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				ds.initDump(tableOrQueryName, keyColumns, md);
				doSyntaxDumpList.add(false);
				filenameList.add(null);
				
				if(ds.usesImportedFKs() || importedFKs!=null) {
					ds.setImportedFKs(importedFKs);
				}
				
				if(ds.usesAllUKs()) {
					ds.setAllUKs(uniqueKeys);
				} 
				
				if(ds.isWriterIndependent()) { 
					doSyntaxDumpList.set(i, true);
					continue;
				}
				
				String filename = getDynamicFileName(prop, tableOrQueryId, ds.getSyntaxId());
				
				if(filename==null) {
					log.warn("no output file defined for syntax '"+ds.getSyntaxId()+"'");
				}
				else {
					filename = filename.replaceAll(FILENAME_PATTERN_TABLE_QUERY_ID, tableOrQueryId);
					filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, tableOrQueryName);
					filename = filename.replaceAll(FILENAME_PATTERN_SYNTAXFILEEXT, ds.getDefaultFileExtension());
					
					doSyntaxDumpList.set(i, true);
					filenameList.set(i, filename);
				}
			}
			
			Map<String, String> lastPartitionIdByPartitionPattern = new HashMap<String, String>();
			Map<String, DumpSyntax> statefulDumpSyntaxes = new HashMap<String, DumpSyntax>();
			Map<String, Long> countInPartitionByPattern = new HashMap<String, Long>();
			for(int partIndex = 0; partIndex<partitionByPatterns.length ; partIndex++) {
				countInPartitionByPattern.put(partitionByPatterns[partIndex], 0l);
			}
			
			//rows
			do {
				for(int partIndex = 0; partIndex<partitionByPatterns.length ; partIndex++) {
					String partitionByPattern = partitionByPatterns[partIndex];
					//log.info("row:: partitionby:: "+partitionByPattern);
					List<String> partitionByCols = getPartitionCols(partitionByPattern);
					long countInPartition = countInPartitionByPattern.get(partitionByPattern);
					
					partitionByStrId = getPartitionByStr(partitionByPattern, rs, partitionByCols);
					
					for(int i=0;i<syntaxList.size();i++) {
						DumpSyntax ds = syntaxList.get(i);
						if(doSyntaxDumpList.get(i)) {
							if(ds.isWriterIndependent()) {
								ds.dumpRow(rs, countInPartition, null);
								continue;
							}
							
							if(ds.isStateful()) {
								String dskey = ds.getSyntaxId()+"$"+partitionByPattern;
								DumpSyntax ds2 = statefulDumpSyntaxes.get(dskey);
								if(ds2==null) {
									ds2 = (DumpSyntax) ds.clone();
									statefulDumpSyntaxes.put(dskey, ds2);
								}
								ds = ds2;
							}
							
							String finalFilename = getFinalFilenameForAbstractFilename(filenameList.get(i), partitionByStrId);
							boolean newFilename = isSetNewFilename(writersOpened, finalFilename, partitionByPattern, charset, writeBOM, writeAppend);
							Writer w = writersOpened.get(getWriterMapKey(finalFilename, partitionByPattern));
								
							String lastPartitionId = lastPartitionIdByPartitionPattern.get(partitionByPattern);
							if(lastPartitionId!=null && !partitionByStrId.equals(lastPartitionId)) {
								String lastFinalFilename = getFinalFilenameForAbstractFilename(filenameList.get(i), lastPartitionId);
								//log.info("partid>> "+lastPartitionId+" // "+partitionByStrId+" // "+lastFinalFilename+" // "+countInPartition);
								countInPartition = 0;
								String lastWriterMapKey = getWriterMapKey(lastFinalFilename, partitionByPattern);
								closeWriter(writersOpened, writersSyntaxes, lastWriterMapKey);
								removeWriter(writersOpened, writersSyntaxes, lastWriterMapKey);
							}
							
							if(newFilename) {
								//if(lastWriterMapKey!=null) { ds.flushBuffer(writersOpened.get(lastWriterMapKey)); }
								log.debug("new filename="+finalFilename+" [charset="+charset+"]");
								ds.dumpHeader(w);
								writersSyntaxes.put(finalFilename, ds);
							}
							try {
								ds.dumpRow(rs, countInPartition, w);
							}
							catch(SQLException e) {
								log.warn("error dumping row "+(count+1)+" from query '"+tableOrQueryId+"/"+tableOrQueryName+"': syntax "+ds.getSyntaxId()+" disabled");
								log.info("stack...",e);
								syntaxList.remove(i); i--;
								conn.rollback();
							}
						}
					}
					lastPartitionIdByPartitionPattern.put(partitionByPattern, partitionByStrId);
					countInPartitionByPattern.put(partitionByPattern, ++countInPartition);
				}
				count++;
				
				if(count==1) {
					dump1stRowTime = System.currentTimeMillis();
					if(log1stRow) {
						logRow.info("[qid="+tableOrQueryId+"] 1st row dumped" + 
							" ["+(dump1stRowTime-initTime)+"ms elapsed]");
					}
				}
				if( (logEachXRows>0) && (count%logEachXRows==0) ) { 
					logRow.info("[qid="+tableOrQueryId+"] "+count+" rows dumped"
							+(logNumberOfOpenedWriters?" ["+writersOpened.size()+" opened writers]":"") );
				}
				if(rowlimit<=count) { break; }
			}
			while(rs.next());
			
			long elapsedMilis = System.currentTimeMillis()-initTime;
			
			log.info("dumped "+count+" rows from table/query: "+tableOrQueryName + 
				(rs.next()?" (more rows exists)":"") + 
				" ["+elapsedMilis+"ms elapsed]" +
				(elapsedMilis>0?" ["+( (count*1000)/elapsedMilis )+" rows/s]":"")
				);

			//footer
			Set<String> filenames = writersOpened.keySet();
			for(String filename: filenames) {
				//for(String partitionByPattern: partitionByPatterns) {
				closeWriter(writersOpened, writersSyntaxes, filename);
			}
			writersOpened.clear();
			writersSyntaxes.clear();
			log.debug("wrote all footers for table/query: "+tableOrQueryName);
			
			rs.close();
	}
	
	static void closeWriter(Map<String, Writer> writersOpened, Map<String, DumpSyntax> writersSyntaxes, String key) throws IOException {
		Writer w = writersOpened.get(key);
		String filename = getFilenameFromWriterMapKey(key);
		//String key = getWriterMapKey(filename, partitionByPattern);
		
		DumpSyntax ds = writersSyntaxes.get(filename);
		try {
			ds.dumpFooter(w);
			w.close();
			//log.info("closed stream; filename: "+filename);
		}
		catch(Exception e) {
			log.warn("error closing stream: "+w+"; filename: "+filename, e);
			log.debug("error closing stream: ", e);
		}
	}
	
	static void removeWriter(Map<String, Writer> writersOpened, Map<String, DumpSyntax> writersSyntaxes, String key) throws IOException {
		String filename = getFilenameFromWriterMapKey(key);
		
		Writer writerRemoved = writersOpened.remove(key);
		if(writerRemoved==null) { log.warn("writer for file '"+filename+"' not found"); }
		DumpSyntax syntaxRemoved = writersSyntaxes.remove(filename);
		if(syntaxRemoved==null) { log.warn("syntax for file '"+filename+"' not found"); }
	}
	
	static String getDynamicFileName(Properties prop, String tableOrQueryId, String syntaxId) {
		log.debug("getDynamicOutFileName: id="+tableOrQueryId+"; syntax="+syntaxId);
		String filename = prop.getProperty(PROP_DATADUMP_OUTFILEPATTERN+".id@"+tableOrQueryId+".syntax@"+syntaxId);
		if(filename!=null) { return filename; }

		filename = prop.getProperty(PROP_DATADUMP_OUTFILEPATTERN+".id@"+tableOrQueryId);
		if(filename!=null) { return filename; }
		
		filename = prop.getProperty(PROP_DATADUMP_OUTFILEPATTERN+".syntax@"+syntaxId);
		if(filename!=null) { return filename; }

		filename = prop.getProperty(PROP_DATADUMP_OUTFILEPATTERN);
		if(filename!=null) { return filename; }
		
		log.warn("no '"+PROP_DATADUMP_OUTFILEPATTERN+"' defined");
		return null;
	}
	
	static String getFinalFilenameForAbstractFilename(String filenameAbstract, String partitionByStr) throws UnsupportedEncodingException, FileNotFoundException {
		return filenameAbstract.replaceAll("\\$\\{partitionby\\}", partitionByStr);
	}
	
	/*Writer getWriterForFilename(String filename, String charset, boolean append) throws UnsupportedEncodingException, FileNotFoundException {
		//String filename = getNewForFilename(filenameAbstract, partitionByStr);
		/*boolean alreadyOpened = filesOpened.contains(filename);
		if(!alreadyOpened) { 
			filesOpened.add(filename);
			log.debug("new file out: "+filename);
		}*
		//Writer w = new OutputStreamWriter(new FileOutputStream(filename, true), charset);
		Writer w = new OutputStreamWriter(new FileOutputStream(filename, append), charset);
		return w;
	}*/
	
	static String getWriterMapKey(String fname, String partitionBy) {
		return fname+"$$"+partitionBy;
	}

	static String getFilenameFromWriterMapKey(String key) {
		return key.substring(0, key.indexOf("$$"));
	}
	
	static boolean isSetNewFilename(Map<String, Writer> writersOpened, String fname, String partitionBy, String charset, Boolean writeBOM, boolean append) throws UnsupportedEncodingException, FileNotFoundException {
		String key = getWriterMapKey(fname, partitionBy);
		if(! writersOpened.containsKey(key)) {
			File f = new File(fname);
			File parent = f.getParentFile();
			if(!parent.isDirectory()) {
				logDir.debug("creating dir: "+parent);
				parent.mkdirs();
			}

			//see: http://stackoverflow.com/questions/9852978/write-a-file-in-utf-8-using-filewriter-java
			// http://stackoverflow.com/questions/1001540/how-to-write-a-utf-8-file-with-java
			CharsetEncoder encoder = Charset.forName(charset).newEncoder();
			encoder.onMalformedInput(CodingErrorAction.REPLACE);
			encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
			
			OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(fname, append), encoder);
			writeBOMifNeeded(w, charset, writeBOM);
			writersOpened.put(key, w);
			//filesOpened.add(fname);
			return true;
		}
		return false;
	}
	
	/*
	 * see:
	 * http://en.wikipedia.org/wiki/Byte_order_mark
	 * http://stackoverflow.com/questions/4389005/how-to-add-a-utf-8-bom-in-java
	 */
	static void writeBOMifNeeded(Writer w, String charset, Boolean doWrite) {
		try {
			if(DataDumpUtils.CHARSET_UTF8.equalsIgnoreCase(charset)) {
				if(doWrite!=null && doWrite==true) {
					w.write('\ufeff');
				}
			}
			/*else if("ISO-8859-1".equalsIgnoreCase(charset)) {
				//do nothing
			}*/
			else {
				if(doWrite!=null && doWrite==true) {
					log.warn("unknown BOM for charset '"+charset+"'");
				}
			}
		} catch (IOException e) {
			log.warn("error writing BOM [charset="+charset+"]: "+e);
		}
	}
	
	static List<String> getPartitionCols(String partitionByPattern) {
		List<String> rets = new ArrayList<String>();
		String sMatcher = "\\$\\{col:(.+?)\\}";
		Matcher m = Pattern.compile(sMatcher).matcher(partitionByPattern);
		while(m.find()) {
			rets.add(m.group(1));
		}
		return rets;
	}
	
	static String getPartitionByStr(String partitionByStr, ResultSet rs, List<String> cols) throws SQLException {
		//XXX: numberformatter (leading 0s) for partitionId?
		//XXX: add dataformatter? useful for partitioning by year, year-month, ... 
		for(String c: cols) {
			String replacement = null;
			try {
				replacement = rs.getString(c);
			}
			catch(SQLException e) {
				log.warn("getPartitionByStr(): column '"+c+"' not found in result set");
				throw e;
			}
			if(replacement==null) { replacement = ""; }
			partitionByStr = partitionByStr.replaceAll("\\$\\{col:"+c+"\\}", replacement);
		}
		return partitionByStr;
	}
	
	static DumpSyntax getObjectOfClass(List<? extends DumpSyntax> l, Class<?> c) {
		for(Object o: l) {
			if(c.isAssignableFrom(o.getClass())) { return (DumpSyntax) o; }
		}
		return null;
	}

	static List<String> getTables4dump(Properties prop) {
		String tables4dumpProp = prop.getProperty(PROP_DATADUMP_TABLES);
		if(tables4dumpProp!=null) {
			List<String> tables4dump = new ArrayList<String>();
			String[] tables4dumpArr = tables4dumpProp.split(",");
			for(String s: tables4dumpArr) {
				tables4dump.add(s.trim());
			}
			log.debug("tables for dump filter: "+tables4dump);
			return tables4dump;
		}
		return null;
	}
	
}
