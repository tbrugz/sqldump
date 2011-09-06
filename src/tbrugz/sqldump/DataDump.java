package tbrugz.sqldump;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import tbrugz.sqldump.datadump.DumpSyntax;
import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.Table;

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
 */
public class DataDump {

	//generic props
	static final String PROP_DATADUMP_OUTFILEPATTERN = "sqldump.datadump.outfilepattern";
	//static final String PROP_DATADUMP_INSERTINTO = "sqldump.datadump.useinsertintosyntax";
	static final String PROP_DATADUMP_SYNTAXES = "sqldump.datadump.dumpsyntaxes";
	static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.charset";
	static final String PROP_DATADUMP_ROWLIMIT = "sqldump.datadump.rowlimit";
	static final String PROP_DATADUMP_TABLES = "sqldump.datadump.tables";
	static final String PROP_DATADUMP_DATEFORMAT = "sqldump.datadump.dateformat";
	static final String PROP_DATADUMP_ORDERBYPK = "sqldump.datadump.orderbypk";

	//defaults
	static final String CHARSET_DEFAULT = "UTF-8";
	
	static final String FILENAME_PATTERN_TABLE_QUERY_ID = "\\$\\{id\\}";
	static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	//static final String FILENAME_PATTERN_QUERYNAME = "\\$\\{queryname\\}";
	static final String FILENAME_PATTERN_SYNTAXFILEEXT = "\\$\\{syntaxfileext\\}";
	
	static Logger log = Logger.getLogger(DataDump.class);
	static Logger logDir = Logger.getLogger(DataDump.class.getName()+".datadump-dir");
	static Logger logRow = Logger.getLogger(DataDump.class.getName()+".datadump-row");
	
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

	public void dumpData(Connection conn, Collection<Table> tablesForDataDump, Properties prop) throws Exception {
		log.info("data dumping...");
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		
		String dateFormat = prop.getProperty(PROP_DATADUMP_DATEFORMAT);
		if(dateFormat!=null) {
			Utils.dateFormatter = new SimpleDateFormat(dateFormat);
		}
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean orderByPK = Utils.getPropBool(prop, PROP_DATADUMP_ORDERBYPK);

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

		for(Table table: tablesForDataDump) {
			String tableName = table.name;
			if(tables4dump!=null) {
				if(!tables4dump.contains(tableName)) { continue; }
				else { tables4dump.remove(tableName); }
			}
			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.datadump."+tableName+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			String whereClause = prop.getProperty("sqldump.datadump."+tableName+".where");
			String selectColumns = prop.getProperty("sqldump.datadump."+tableName+".columns");
			if(selectColumns==null) { selectColumns = "*"; }
			String orderClause = prop.getProperty("sqldump.datadump."+tableName+".order");
			if(orderClause==null && orderByPK) { 
				Constraint ctt = table.getPKConstraint();
				if(ctt!=null) {
					orderClause = Utils.join(ctt.uniqueColumns, ", ");
				}
				else {
					log.warn("table '"+tableName+"' has no PK for datadump ordering");
				}
			}

			log.debug("dumping data/inserts from table: "+tableName);
			String sql = "select "+selectColumns+" from \""+tableName+"\""
					+ (whereClause!=null?" where "+whereClause:"")
					+ (orderClause!=null?" order by "+orderClause:"");
			log.debug("sql: "+sql);
			
			//XXX: table dump with partitionBy?
			runQuery(conn, sql, null, prop, tableName, tableName, charset, 
					rowlimit, 
					syntaxList
					);
		}
		
		if(tables4dump.size()>0) {
			log.warn("tables selected for dump but not found: "+Utils.join(tables4dump, ", "));
		}
	}
	
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop, 
			String tableOrQueryId, String tableOrQueryName, String charset, long rowlimit, List<DumpSyntax> syntaxList
			) throws Exception {
		runQuery(conn, sql, params, prop, tableOrQueryId, tableOrQueryName, charset, rowlimit, syntaxList, null);
	}
		
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop, String tableOrQueryId, String tableOrQueryName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList,
			String partitionByPattern
			) throws Exception {
		
			PreparedStatement st = conn.prepareStatement(sql);
			//st.setFetchSize(20);
			if(params!=null) {
				for(int i=0;i<params.size();i++) {
					st.setString(i+1, params.get(i));
				}
			}
			ResultSet rs = st.executeQuery();
			ResultSetMetaData md = rs.getMetaData();

			boolean hasData = rs.next();
			//so empty tables do not create empty dump files
			if(!hasData) return;
			
			//String defaultFilename = prop.getProperty(PROP_DATADUMP_OUTFILEPATTERN);

			List<String> filenameList = new ArrayList<String>();
			List<Boolean> doSyntaxDumpList = new ArrayList<Boolean>();
			
			//String partitionBy = prop.getProperty(PROP_DATADUMP_FILEPATTERN);
			if(partitionByPattern==null) { partitionByPattern = ""; }
			List<String> partitionByCols = getPartitionCols(partitionByPattern);
			
			String partitionByStrId = "";
			String partitionByStrIdOld = "";
			
			Map<String, Writer> writersOpened = new HashMap<String, Writer>();
			Map<String, DumpSyntax> writersSyntaxes = new HashMap<String, DumpSyntax>();
			
			//XXX: prop for setting 'logEachXRows'
			long logEachXRows = 10000;

			//header
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				ds.initDump(tableOrQueryName, md);
				doSyntaxDumpList.add(false);
				filenameList.add(null);
				
				//String filename = prop.getProperty("sqldump.datadump."+ds.getSyntaxId()+".filepattern", defaultFilename);
				String filename = getDynamicFileName(prop, tableOrQueryId, ds.getSyntaxId());
				
				if(filename==null) {
					log.warn("no output file defined for syntax '"+ds.getSyntaxId()+"'");
				}
				else {
					filename = filename.replaceAll(FILENAME_PATTERN_TABLE_QUERY_ID, tableOrQueryId);
					filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, tableOrQueryName);
					filename = filename.replaceAll(FILENAME_PATTERN_SYNTAXFILEEXT, ds.getDefaultFileExtension());
					
					doSyntaxDumpList.set(i, true);
					//writerList.set(i, new OutputStreamWriter(new FileOutputStream(filename, alreadyOpened), charset));
					filenameList.set(i, filename);

					partitionByStrIdOld = partitionByStrId; 
					partitionByStrId = getPartitionByStr(partitionByPattern, rs, partitionByCols);
					String finalFilename = getFinalFilenameForAbstractFilename(filename, partitionByStrId);
					//Writer w = getWriterForFilename(finalFilename, charset, false);
					boolean newFilename = isSetNewFilename(writersOpened, finalFilename, charset);
					Writer w = writersOpened.get(finalFilename);
					if(newFilename) {
						//should always be true
						log.debug("new filename="+finalFilename);
					}
					else {
						log.warn("filename '"+finalFilename+"' shouldn't have been already opened...");
					}

					writersSyntaxes.put(finalFilename, ds);
					ds.dumpHeader(w);
					//ds.dumpHeader(writerList.get(i));
				}
			}
			
			//rows
			long count = 0;
			long countInPartition = 0;
			do {
				partitionByStrIdOld = partitionByStrId; 
				partitionByStrId = getPartitionByStr(partitionByPattern, rs, partitionByCols);
				boolean partitionChanged = false;
				if(!partitionByStrId.equals(partitionByStrIdOld)) {
					partitionChanged = true;
					countInPartition = 0;
					log.debug("partitionId changed: from='"+partitionByStrIdOld+"' to='"+partitionByStrId+"'");
				}
				
				for(int i=0;i<syntaxList.size();i++) {
					DumpSyntax ds = syntaxList.get(i);
					if(doSyntaxDumpList.get(i)) {
						String finalFilename = getFinalFilenameForAbstractFilename(filenameList.get(i), partitionByStrId);
						//Writer w = getWriterForFilename(finalFilename, charset, true);
						boolean newFilename = isSetNewFilename(writersOpened, finalFilename, charset);
						Writer w = writersOpened.get(finalFilename);
						if(partitionChanged) {
							//for DumpSyntaxes that have buffer (like FFC)
							String finalFilenameOld = getFinalFilenameForAbstractFilename(filenameList.get(i), partitionByStrIdOld);
							ds.flushBuffer(writersOpened.get(finalFilenameOld));
							//XXX: write footer & close file here? (less simultaneous open-files)
							closeWriter(writersOpened, writersSyntaxes, finalFilenameOld);
							removeWriter(writersOpened, writersSyntaxes, finalFilenameOld);
							//w.flush();
						}
						
						if(newFilename) {
							log.debug("new filename="+finalFilename);
							ds.dumpHeader(w);
							writersSyntaxes.put(finalFilename, ds);
						}
						//TODOne: count should be total count or file count? i vote on file count :) (FFC uses it for buffering)
						ds.dumpRow(rs, countInPartition, w);
						//ds.dumpRow(rs, count, writerList.get(i));
					}
				}
				count++;
				countInPartition++;
				
				if( (logEachXRows>0) && (count%logEachXRows==0) ) { 
					logRow.info("[qid="+tableOrQueryId+"] "+count+" rows dumped");
				}
				if(rowlimit<=count) { break; }
			}
			while(rs.next());
			log.info("dumped "+count+" rows from table/query: "+tableOrQueryName);

			//footer
			Set<String> filenames = writersOpened.keySet();
			for(String filename: filenames) {
				closeWriter(writersOpened, writersSyntaxes, filename);
				/*Writer w = writersOpened.get(filename);
				DumpSyntax ds = writersSyntaxes.get(filename);
				try {
					ds.dumpFooter(w);
				}
				catch(Exception e) {
					log.warn("error closing stream: "+w+"; filename: "+filename);
					log.debug("error closing stream: ", e);
				}
				w.close();*/
			}
			writersOpened.clear();
			writersSyntaxes.clear();
			log.debug("wrote all footers for table/query: "+tableOrQueryName);
			
			/*
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				if(doSyntaxDumpList.get(i)) {
					//partitionByStrId = getPartitionByStr(partitionByPattern, rs, partitionByCols);
					String finalFilename = getFinalFilenameForAbstractFilename(writerList.get(i), partitionByStrId);
					//Writer w = getWriterForFilename(finalFilename, charset);
					Writer w = writersOpened.get(finalFilename);
					ds.dumpFooter(w);
					//ds.dumpFooter(writerList.get(i));
					
					//writerList.get(i).close();
				}
			}
			
			for(String s: writersOpened.keySet()) {
				writersOpened.get(s).close();
			}
			*/
			
			rs.close();
	}
	
	void closeWriter(Map<String, Writer> writersOpened, Map<String, DumpSyntax> writersSyntaxes, String filename) throws IOException {
		Writer w = writersOpened.get(filename);
		DumpSyntax ds = writersSyntaxes.get(filename);
		try {
			ds.dumpFooter(w);
		}
		catch(Exception e) {
			log.warn("error closing stream: "+w+"; filename: "+filename);
			log.debug("error closing stream: ", e);
		}
		w.close();
	}
	
	void removeWriter(Map<String, Writer> writersOpened, Map<String, DumpSyntax> writersSyntaxes, String filename) throws IOException {
		Writer writerRemoved = writersOpened.remove(filename);
		if(writerRemoved==null) { log.warn("writer for file '"+filename+"' not found"); }
		DumpSyntax syntaxRemoved = writersSyntaxes.remove(filename);
		if(syntaxRemoved==null) { log.warn("syntax for file '"+filename+"' not found"); }
	}
	
	String getDynamicFileName(Properties prop, String tableOrQueryId, String syntaxId) {
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
	
	String getFinalFilenameForAbstractFilename(String filenameAbstract, String partitionByStr) throws UnsupportedEncodingException, FileNotFoundException {
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
	
	boolean isSetNewFilename(Map<String, Writer> writersOpened, String fname, String charset) throws UnsupportedEncodingException, FileNotFoundException {
		if(! writersOpened.containsKey(fname)) {
			File f = new File(fname);
			File parent = f.getParentFile();
			if(!parent.isDirectory()) {
				logDir.debug("creating dir: "+parent);
				parent.mkdirs();
			}
			OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(fname, false), charset); //XXX: false: never append
			writersOpened.put(fname, w);
			//filesOpened.add(fname);
			return true;
		}
		return false;
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
		for(String c: cols) {
			String replacement = rs.getString(c);
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
