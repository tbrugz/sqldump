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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import tbrugz.sqldump.dbmodel.Constraint;
import tbrugz.sqldump.dbmodel.DBObject;
import tbrugz.sqldump.dbmodel.DBObjectType;
import tbrugz.sqldump.dbmodel.FK;
import tbrugz.sqldump.dbmodel.ModelUtils;
import tbrugz.sqldump.dbmodel.Table;
import tbrugz.sqldump.dbmodel.TableType;
import tbrugz.sqldump.def.AbstractSQLProc;
import tbrugz.sqldump.def.DBMSResources;
import tbrugz.sqldump.def.Defs;
import tbrugz.sqldump.def.ProcessingException;
import tbrugz.sqldump.resultset.ResultSetDecoratorFactory;
import tbrugz.sqldump.util.CategorizedOut;
import tbrugz.sqldump.util.SQLUtils;
import tbrugz.sqldump.util.StringDecorator;
import tbrugz.sqldump.util.Utils;
import tbrugz.util.LongFactory;
import tbrugz.util.NonNullGetMap;

/*
 * TODO: floatFormatter!
 * TODO: option to include, or not, partition columns in output
 * XXX: partition by schemaname?
 */
public class DataDump extends AbstractSQLProc {

	//prefix
	static final String DATADUMP_PROP_PREFIX = "sqldump.datadump.";
	
	//suffixes
	static final String SUFFIX_DATADUMP_WRITEBOM = "writebom";
	
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
	static final String PROP_DATADUMP_WRITEBOM = DATADUMP_PROP_PREFIX+SUFFIX_DATADUMP_WRITEBOM;
	static final String PROP_DATADUMP_WRITEAPPEND = "sqldump.datadump.writeappend";
	static final String PROP_DATADUMP_CREATEEMPTYFILES = "sqldump.datadump.createemptyfiles";
	static final String PROP_DATADUMP_PARTITIONBY_DATEFORMAT = "sqldump.datadump.partitionby.dateformat";

	//defaults
	static final String CHARSET_DEFAULT = DataDumpUtils.CHARSET_UTF8;
	static final long LOG_EACH_X_ROWS_DEFAULT = 50000;
	
	static final String PATTERN_TABLE_QUERY_ID = "id";
	static final String PATTERN_PARTITIONBY = "partitionby";
	static final String PATTERN_SYNTAXFILEEXT = "syntaxfileext"; //syntaxdefaultfileext, defaultsyntaxfileext, defaultfileext, fileext
	
	static final String PATTERN_TABLE_QUERY_ID_FINAL = Pattern.quote(Defs.addSquareBraquets(PATTERN_TABLE_QUERY_ID));
	public static final String PATTERN_TABLENAME_FINAL = Pattern.quote(Defs.addSquareBraquets(Defs.PATTERN_TABLENAME));
	static final String PATTERN_PARTITIONBY_FINAL = Pattern.quote(Defs.addSquareBraquets(PATTERN_PARTITIONBY));
	static final String PATTERN_SYNTAXFILEEXT_FINAL = Pattern.quote(Defs.addSquareBraquets(PATTERN_SYNTAXFILEEXT));
	//XXX add [schema] pattern
	//XXX add [tabletype] pattern - TABLE, VIEW, QUERY ?
	//XXX add [syntaxid] pattern - may be different from [syntaxfileext]
		
	@Deprecated
	static final String FILENAME_PATTERN_TABLE_QUERY_ID = "\\$\\{id\\}";
	@Deprecated
	public static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	@Deprecated
	static final String FILENAME_PATTERN_PARTITIONBY = "\\$\\{partitionby\\}";
	@Deprecated
	public static final String FILENAME_PATTERN_SYNTAXFILEEXT = "\\$\\{syntaxfileext\\}";
	
	private static final Log log = LogFactory.getLog(DataDump.class);
	private static final Log logDir = LogFactory.getLog(DataDump.class.getName()+".datadump-dir");
	private static final Log logNewFile = LogFactory.getLog(DataDump.class.getName()+".datadump-file");
	private static final Log log1stRow = LogFactory.getLog(DataDump.class.getName()+".datadump-1st");
	private static final Log logRow = LogFactory.getLog(DataDump.class.getName()+".datadump-row");
	
	static DateFormat partitionByDateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	final Set<String> bomWarned = new HashSet<String>();
	
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
	void dumpData(Connection conn, Collection<Table> tablesForDataDump, Properties prop) {
		log.info("data dumping...");
		
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean orderByPK = Utils.getPropBool(prop, PROP_DATADUMP_ORDERBYPK, true);

		List<String> tables4dump = getTables4dump(prop);
		
		List<DumpSyntax> syntaxList = getSyntaxList(prop, PROP_DATADUMP_SYNTAXES);
		if(syntaxList==null) {
			log.error("no datadump syntax(es) defined [prop '"+PROP_DATADUMP_SYNTAXES+"']");
			if(failonerror) {
				throw new ProcessingException("DataDump: no datadump syntax(es) defined");
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
		
		int queriesRunned = 0;
		int ignoredTables = 0;
		
		Collection<Table> tablesForDataDumpLoop = null;
		if(tables4dump==null) {
			tablesForDataDumpLoop = tablesForDataDump;
		}
		else {
			//ordering tables for dump
			tablesForDataDumpLoop = new ArrayList<Table>();
			for(String tName: tables4dump) {
				Table t = DBObject.getDBIdentifiableByTypeAndName(tablesForDataDump, DBObjectType.TABLE, tName);
				if(t==null) {
					log.warn("table '"+tName+"' not found for dump");
					ignoredTables++;
				}
				else {
					tablesForDataDumpLoop.add(t);
				}
			}
			for(Table t: tablesForDataDump) {
				if(!tables4dump.contains(t.getName())) {
					log.debug("ignoring table: "+t.getName()+" [filtered]");
					ignoredTables++;
					continue;
				}
			}
		}
		
		LABEL_TABLE:
		for(Table table: tablesForDataDumpLoop) {
			String tableName = table.getName();
			String schemaName = table.getSchemaName();
			String tableFullName = table.getQualifiedName();
			if(tables4dump!=null) { tables4dump.remove(tableName); }
			if(typesToDump!=null) {
				if(!typesToDump.contains(table.getType())) {
					log.debug("ignoring table '"+tableFullName+"' by type [type="+table.getType()+"]");
					ignoredTables++;
					continue;
				}
			}
			if(ignoretablesregex!=null) {
				for(String tregex: ignoretablesregex) {
					if(tableName.matches(tregex)) {
						log.debug("ignoring table '"+tableFullName+"' by regex [regex="+tregex+"]");
						ignoredTables++;
						continue LABEL_TABLE;
					}
				}
			}
			List<FK> importedFKs = ModelUtils.getImportedKeys(table, model.getForeignKeys());
			List<Constraint> uniqueKeys = ModelUtils.getUKs(table);
			
			long rowlimit = getTableRowLimit(prop, tableName);

			String whereClause = prop.getProperty(DATADUMP_PROP_PREFIX+tableName+".where");
			String selectColumns = prop.getProperty(DATADUMP_PROP_PREFIX+tableName+".columns");
			if(selectColumns==null) { selectColumns = "*"; }
			String orderClause = prop.getProperty(DATADUMP_PROP_PREFIX+tableName+".order");

			List<String> pkCols = null;  
			if(table.getPKConstraint()!=null) {
				pkCols = table.getPKConstraint().getUniqueColumns();
			} 
			
			//String quote = conn.getMetaData().getIdentifierQuoteString();
			String sql = getQuery(table, selectColumns, whereClause, orderClause, orderByPK); //XXX add quote?
			
			try {
				//XXX: table dump with partitionBy?
				runQuery(conn, sql, null, prop, schemaName, tableName, tableName, charset, 
						rowlimit,
						syntaxList,
						null, //partitionby
						pkCols,
						importedFKs,
						uniqueKeys,
						null //decoratorFactory
						);
				queriesRunned++;
			}
			catch(Exception e) {
				log.warn("error dumping data from table: "+tableFullName+"\n\tsql: "+sql+"\n\texception: "+e);
				log.info("exception:", e);
				if(failonerror) {
					throw new ProcessingException(e);
				}
			}
		}
		
		if(tablesForDataDump.size()==0) {
			log.warn("no tables found in model for data dumping...");
		}
		else {
			if(tables4dump!=null && tables4dump.size()>0) {
				log.warn("tables selected for dump but not found: "+Utils.join(tables4dump, ", "));
			}
			log.info("..."+queriesRunned+" queries dumped"
					+(ignoredTables>0?" ["+ignoredTables+" tables ignored]":"") );
		}
	}
	
	//XXX: move to DataDumpUtils?
	public static String getQuery(Table table, String selectColumns, String whereClause, String orderClause, boolean orderByPK) {
		String tableName = table.getName();
		
		String quote = DBMSResources.instance().getIdentifierQuoteString();
		StringDecorator quoteAllDecorator = new StringDecorator.StringQuoterDecorator(quote);
		
		if(orderClause==null && orderByPK) { 
			Constraint ctt = table.getPKConstraint();
			if(ctt!=null) {
				orderClause = Utils.join(ctt.getUniqueColumns(), ", ", quoteAllDecorator);
			}
			else {
				log.warn("table '"+tableName+"' has no PK for datadump ordering");
			}
		}

		log.debug("dumping data/inserts from table: "+tableName);
		//String sql = "select "+selectColumns+" from \""+table.schemaName+"."+tableName+"\""
		
		String sql = "select "+selectColumns
				+" from "+DBObject.getFinalName(table, quoteAllDecorator, true)
				+ (whereClause!=null?" where "+whereClause:"")
				+ (orderClause!=null?" order by "+orderClause:"");
		log.debug("sql: "+sql);

		return sql;
	} 
	
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop,
			String schemaName, String tableOrQueryId, String tableOrQueryName, String[] partitionByPatterns, List<String> keyColumns
			) throws SQLException, IOException {
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		long rowlimit = getTableRowLimit(prop, tableOrQueryName);
		List<DumpSyntax> syntaxList = getSyntaxList(prop, PROP_DATADUMP_SYNTAXES);
		if(syntaxList==null) {
			log.error("no datadump syntax defined");
			if(failonerror) {
				throw new ProcessingException("DataDump: no datadump syntax defined");
			}
		}
		
		runQuery(conn, sql, params, prop, schemaName, tableOrQueryId,
				tableOrQueryName, charset, rowlimit, syntaxList, 
				partitionByPatterns, keyColumns, 
				null, //List<FK> importedFKs
				null, //List<Constraint> uniqueKeys
				null  //ResultSetDecoratorFactory rsDecoratorFactory
				);
	}
	
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop,
			String schemaName, String tableOrQueryId, String tableOrQueryName, String charset,
			long rowlimit, List<DumpSyntax> syntaxList
			) throws SQLException, IOException {
		runQuery(conn, sql, params, prop, schemaName, tableOrQueryId, tableOrQueryName, charset, rowlimit, syntaxList, null, null, null, null, null);
	}
	
	final Set<String> deprecatedPatternWarnedFiles = new HashSet<String>();
	
	void runQuery(Connection conn, String sql, List<String> params, Properties prop, 
			String schemaName, String tableOrQueryId, String tableOrQueryName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList,
			String[] partitionByPatterns,
			List<String> keyColumns,
			List<FK> importedFKs,
			List<Constraint> uniqueKeys,
			ResultSetDecoratorFactory rsDecoratorFactory
			) throws SQLException, IOException {
		PreparedStatement st = conn.prepareStatement(sql);
		try {
			runQuery(conn, st, params, prop, schemaName, tableOrQueryId,
					tableOrQueryName, charset, rowlimit, syntaxList, partitionByPatterns,
					keyColumns, importedFKs, uniqueKeys, rsDecoratorFactory);
		}
		catch(SQLException e) {
			log.warn("error in sql: "+sql);
			throw e;
		}
		finally {
			if(log.isDebugEnabled()) { SQLUtils.logWarnings(st.getWarnings(), log); }
		}
	}
		
	void runQuery(Connection conn, PreparedStatement st, List<String> params, Properties prop, 
			String schemaName, String tableOrQueryId, String tableOrQueryName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList,
			String[] partitionByPatterns,
			List<String> keyColumns,
			List<FK> importedFKs,
			List<Constraint> uniqueKeys,
			ResultSetDecoratorFactory rsDecoratorFactory
			) throws SQLException, IOException {
			//st.setFetchSize(20);
			if(params!=null) {
				for(int i=0;i<params.size();i++) {
					st.setString(i+1, params.get(i));
				}
			}
			
			long initTime = System.currentTimeMillis();
			logRow.debug("[qid="+tableOrQueryId+"] running query '"+tableOrQueryName+"'");
			ResultSet rs = st.executeQuery();
			if(log.isDebugEnabled()) { SQLUtils.logWarnings(rs.getWarnings(), log); }
			if(rsDecoratorFactory!=null) {
				rs = rsDecoratorFactory.getDecoratorOf(rs);
			}
			
			dumpResultSet(rs, prop, schemaName, tableOrQueryId, tableOrQueryName,
					charset, rowlimit, syntaxList, partitionByPatterns,
					keyColumns, importedFKs, uniqueKeys, rsDecoratorFactory,
					initTime);
	}

	public void dumpResultSet(ResultSet rs, Properties prop, 
			String schemaName, String tableOrQueryId, String tableOrQueryName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList,
			String[] partitionByPatterns,
			List<String> keyColumns,
			List<FK> importedFKs,
			List<Constraint> uniqueKeys,
			ResultSetDecoratorFactory rsDecoratorFactory,
			long initTime
			) throws SQLException, IOException {
			
			if(initTime<=0) { initTime = System.currentTimeMillis(); }
			long dump1stRowTime = -1;
			
			ResultSetMetaData md = rs.getMetaData();
			
			if(log.isDebugEnabled()) { //XXX: debug enabled? any better way?
				DataDumpUtils.logResultSetColumnsTypes(md, tableOrQueryName, log);
			}

			boolean createEmptyDumpFiles = Utils.getPropBoolean(prop, PROP_DATADUMP_CREATEEMPTYFILES, false);
			
			String partitionByDF = prop.getProperty(PROP_DATADUMP_PARTITIONBY_DATEFORMAT);
			if(partitionByDF!=null) {
				partitionByDateFormatter = new SimpleDateFormat(partitionByDF);
			}
			
			boolean hasData = rs.next();
			//so empty tables do not create empty dump files
			if(!hasData) {
				log.info("table/query '"+tableOrQueryName+"' returned 0 rows");
				if(!createEmptyDumpFiles) {
					return;
				}
			}
			
			SQLUtils.setupForNewQuery(md.getColumnCount());
			
			Map<String, Writer> writersOpened = new HashMap<String, Writer>();
			Map<String, DumpSyntax> writersSyntaxes = new HashMap<String, DumpSyntax>();
			
			if(partitionByPatterns==null) { partitionByPatterns = new String[]{ "" }; }
			
			List<String> filenameList = new ArrayList<String>();
			List<Boolean> doSyntaxDumpList = new ArrayList<Boolean>();
			
			String partitionByStrId = "";
			
			Boolean writeBOM = Utils.getPropBoolean(prop, PROP_DATADUMP_WRITEBOM, null);
			boolean writeAppend = Utils.getPropBoolean(prop, PROP_DATADUMP_WRITEAPPEND, false);
			
			boolean dolog1stRow = Utils.getPropBool(prop, PROP_DATADUMP_LOG_1ST_ROW, true);
			boolean logNumberOfOpenedWriters = true;
			long logEachXRows = Utils.getPropLong(prop, PROP_DATADUMP_LOG_EACH_X_ROWS, LOG_EACH_X_ROWS_DEFAULT);
			
			long count = 0;
			
			if(charset==null) { charset = CHARSET_DEFAULT; }
			
			try {

			//headers
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				ds.initDump(schemaName, tableOrQueryName, keyColumns, md);
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
					ds.dumpHeader();
					continue;
				}
				
				String filename = getDynamicFileName(prop, tableOrQueryId, ds.getSyntaxId());
				
				if(filename==null) {
					log.warn("no output file defined for syntax '"+ds.getSyntaxId()+"'");
				}
				else {
					String filenameTmp = filename;
					filename = filename.replaceAll(FILENAME_PATTERN_TABLE_QUERY_ID, Matcher.quoteReplacement(tableOrQueryId));
					//if(!filenameTmp.equals(filename)) { log.warn("using deprecated pattern '${xxx}': "+FILENAME_PATTERN_TABLE_QUERY_ID); filenameTmp = filename; }
					filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, Matcher.quoteReplacement(tableOrQueryName));
					//if(!filenameTmp.equals(filename)) { log.warn("using deprecated pattern '${xxx}': "+FILENAME_PATTERN_TABLENAME); filenameTmp = filename; }
					filename = filename.replaceAll(FILENAME_PATTERN_SYNTAXFILEEXT, ds.getDefaultFileExtension());
					if(!filenameTmp.equals(filename) && !deprecatedPatternWarnedFiles.contains(filenameTmp)) {
						deprecatedPatternWarnedFiles.add(filenameTmp);
						log.warn("using deprecated pattern '${xxx}': "
							+FILENAME_PATTERN_TABLE_QUERY_ID+", "+FILENAME_PATTERN_TABLENAME+", "+FILENAME_PATTERN_PARTITIONBY+" or "+FILENAME_PATTERN_SYNTAXFILEEXT
							+" [filename="+filenameTmp+"]"); // filenameTmp = filename;
					}
					filename = filename.replaceAll(PATTERN_TABLE_QUERY_ID_FINAL, Matcher.quoteReplacement(tableOrQueryId));
					filename = filename.replaceAll(PATTERN_TABLENAME_FINAL, Matcher.quoteReplacement(tableOrQueryName));
					filename = filename.replaceAll(PATTERN_SYNTAXFILEEXT_FINAL, Matcher.quoteReplacement(ds.getDefaultFileExtension()));
					
					doSyntaxDumpList.set(i, true);
					filenameList.set(i, filename);
				}
				
				if(ds.isFetcherSyntax() && syntaxList.size()>1) {
					log.warn("Dump syntax '"+ds.getSyntaxId()+"' is fetcher syntax but other syntaxes are selected [syntaxList="+getSyntaxListNames(syntaxList)+"]");
				}
			}
			
			//Map<String, String> lastPartitionIdByPartitionPattern = new HashMap<String, String>();
			Map<String, DumpSyntax> statefulDumpSyntaxes = new HashMap<String, DumpSyntax>();
			//Map<String, Long> countInPartitionByPattern = new NonNullGetMap<String, Long>(new HashMap<String, Long>(), new LongFactory());
			Map<String, Long> countByPatternFinalFilename = new NonNullGetMap<String, Long>(new HashMap<String, Long>(), new LongFactory());
			
			//rows
			do {
				for(int partIndex = 0; partIndex<partitionByPatterns.length; partIndex++) {
					String partitionByPattern = partitionByPatterns[partIndex];
					//log.info("row:: partitionby:: "+partitionByPattern);
					List<String> partitionByCols = getPartitionCols(partitionByPattern);
					
					partitionByStrId = getPartitionByStr(partitionByPattern, rs, partitionByCols);
					//String countInPartitionKey = partitionByPattern+"$"+partitionByStrId;
					//long countInPartition = countInPartitionByPattern.get(countInPartitionKey);
					
					for(int i=0;i<syntaxList.size();i++) {
						DumpSyntax ds = syntaxList.get(i);
						if(doSyntaxDumpList.get(i)) {
							if(ds.isWriterIndependent()) {
								ds.dumpRow(rs, count); //writer indepentend syntax should not care abount 'countInPartition' line number, right?
								continue;
							}
							
							if(!ds.isPartitionable() && !"".equals(partitionByPattern)) {
								throw new RuntimeException("Dump syntax '"+ds.getSyntaxId()+"' is not partitionable but partition pattern defined [partitionPattern="+partitionByPattern+"]");
							}
							
							if(ds.isStateful() && !"".equals(partitionByPattern)) {
								String dskey = ds.getSyntaxId()+"$"+partitionByPattern;
								DumpSyntax ds2 = statefulDumpSyntaxes.get(dskey);
								if(ds2==null) {
									try {
										ds2 = (DumpSyntax) ds.clone();
									} catch (CloneNotSupportedException e) {
										throw new IOException("Error cloning syntax '"+ds.getSyntaxId()+"' [partitionPattern="+partitionByPattern+"]", e);
									}
									statefulDumpSyntaxes.put(dskey, ds2);
								}
								ds = ds2;
							}
							
							String finalFilename = getFinalFilenameForAbstractFilename(filenameList.get(i), partitionByStrId);
							boolean newFilename = count==0;
							Writer w = CategorizedOut.getStaticWriter(filenameList.get(i));
							if(w==null) {
								Boolean syntaxWriteBOM = Utils.getPropBoolean(prop, DATADUMP_PROP_PREFIX+ds.getSyntaxId()+"."+SUFFIX_DATADUMP_WRITEBOM, writeBOM);
								if(syntaxWriteBOM!=null && syntaxWriteBOM && !ds.allowWriteBOM() && !bomWarned.contains(ds.getSyntaxId())) {
									log.warn("syntax '"+ds.getSyntaxId()+"' should not write BOM");
									bomWarned.add(ds.getSyntaxId());
								}
								//if(syntaxWriteBOM==null && ds.shouldNotWriteBOM()) { syntaxWriteBOM = false; }
								newFilename = isSetNewFilename(writersOpened, finalFilename, partitionByPattern, charset, syntaxWriteBOM, writeAppend);
								w = writersOpened.get(getWriterMapKey(finalFilename, partitionByPattern));
							}
							long countInFilename = countByPatternFinalFilename.get(finalFilename);
							
							//XXX: close writers? there will be problems if query is not ordered by columns used by [partitionby]
							
							/*String lastPartitionId = lastPartitionIdByPartitionPattern.get(partitionByPattern);
							if(lastPartitionId!=null && lastPartitionIdByPartitionPattern.size()>1 && !partitionByStrId.equals(lastPartitionId)) {
								String lastFinalFilename = getFinalFilenameForAbstractFilename(filenameList.get(i), lastPartitionId);
								//log.debug("partid >> "+lastPartitionId+" // "+partitionByStrId+" // "+lastFinalFilename+" // "+countInPartition);
								countInPartition = 0;
								String lastWriterMapKey = getWriterMapKey(lastFinalFilename, partitionByPattern);
								closeWriter(writersOpened, writersSyntaxes, lastWriterMapKey);
								removeWriter(writersOpened, writersSyntaxes, lastWriterMapKey);
							}*/
							
							if(newFilename) {
								//if(lastWriterMapKey!=null) { ds.flushBuffer(writersOpened.get(lastWriterMapKey)); }
								logNewFile.debug("new filename="+finalFilename+" [charset="+charset+"]");
								ds.dumpHeader(w);
								writersSyntaxes.put(finalFilename, ds);
							}
							try {
								if(hasData) {
									ds.dumpRow(rs, countInFilename, w);
									countByPatternFinalFilename.put(finalFilename, ++countInFilename);
								}
								else {
									log.debug("no data to dump to file '"+finalFilename+"'");
								}
							}
							catch(SQLException e) {
								log.warn("error dumping row "+(count+1)+" from query '"+tableOrQueryId+"/"+tableOrQueryName+"': syntax "+ds.getSyntaxId()+" disabled");
								log.info("stack...",e);
								syntaxList.remove(i); i--;
								conn.rollback();
							}
						}
					}
					//lastPartitionIdByPartitionPattern.put(partitionByPattern, partitionByStrId);
					//countInPartitionByPattern.put(countInPartitionKey, ++countInPartition);
				}

				if(hasData) {
					count++;
				}
				
				if(count==1) {
					dump1stRowTime = System.currentTimeMillis();
					if(dolog1stRow) {
						log1stRow.debug("[qid="+tableOrQueryId+"] 1st row dumped"
							+ " ["+(dump1stRowTime-initTime)+"ms elapsed]");
					}
				}
				if( (logEachXRows>0) && (count>0) &&(count%logEachXRows==0) ) { 
					logRow.debug("[qid="+tableOrQueryId+"] "+count+" rows dumped"
							+(logNumberOfOpenedWriters?" ["+writersOpened.size()+" opened writers]":"") );
				}
				if(rowlimit<=count) { break; }
			}
			while(rs.next());
			
			long elapsedMilis = System.currentTimeMillis()-initTime;
			
			log.info("dumped "+count+" rows from table/query: "+tableOrQueryName
				+ (rs.next()?" (more rows exists)":"")
				+ " ["+elapsedMilis+"ms elapsed]"
				+ (elapsedMilis>0?" ["+( (count*1000)/elapsedMilis )+" rows/s]":"")
				);

			//footers
			Set<String> filenames = writersOpened.keySet();
			for(String filename: filenames) {
				//for(String partitionByPattern: partitionByPatterns) {
				//FIXedME: should be count for this file/partition, not resultset. Last countInPartition would work for last partition only
				closeWriter(writersOpened, writersSyntaxes, filename, countByPatternFinalFilename);
			}
			writersOpened.clear();
			writersSyntaxes.clear();
			
			//other footers
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				if(doSyntaxDumpList.get(i)) {
					//writer-independent footers
					if(ds.isWriterIndependent()) {
						ds.dumpFooter(count);
					}
					else {
						//static writers footers
						Writer w = CategorizedOut.getStaticWriter(filenameList.get(i));
						if(w!=null) {
							ds.dumpFooter(count, w);
						}
					}
				}
			}
			
			log.debug("wrote all footers for table/query: "+tableOrQueryName);
			
			rs.close();
			
			}
			catch(IOException e) {
				/*
				 * too many open files? see ulimit
				 * http://posidev.com/blog/2009/06/04/set-ulimit-parameters-on-ubuntu/
				 * https://confluence.atlassian.com/display/CONF29/Fix+'Too+many+open+files'+error+on+Linux+by+increasing+filehandles
				 * http://stackoverflow.com/questions/13988780/too-many-open-files-ulimit-already-changed
				 */
				log.warn("IOException occured ["+writersOpened.size()+" opened writers]: "+e);
				throw e;
			}
			catch(SQLException e) {
				log.warn("SQLException occured [count="+count+"]: "+e);
				throw e;
			}
	}
	
	static void closeWriter(Map<String, Writer> writersOpened, Map<String, DumpSyntax> writersSyntaxes, String key, Map<String, Long> countByPatternFinalFilename) throws IOException {
		Writer w = writersOpened.get(key);
		String filename = getFilenameFromWriterMapKey(key);
		//String key = getWriterMapKey(filename, partitionByPattern);
		long rowsDumped = countByPatternFinalFilename.get(filename);

		DumpSyntax ds = writersSyntaxes.get(filename);
		try {
			ds.dumpFooter(rowsDumped, w);
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
		//log.debug("getDynamicOutFileName: id="+tableOrQueryId+"; syntax="+syntaxId);
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
		return filenameAbstract
				.replaceAll(FILENAME_PATTERN_PARTITIONBY, Matcher.quoteReplacement(partitionByStr))
				.replaceAll(PATTERN_PARTITIONBY_FINAL, Matcher.quoteReplacement(partitionByStr));
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
		//log.debug("isSet: "+key+" ; contains = "+writersOpened.containsKey(key));
		if(! writersOpened.containsKey(key)) {
			File f = new File(fname);
			File parent = f.getParentFile();
			if(parent!=null && !parent.isDirectory()) {
				logDir.debug("creating dir: "+parent);
				parent.mkdirs();
			}

			// see: http://stackoverflow.com/questions/9852978/write-a-file-in-utf-8-using-filewriter-java
			//      http://stackoverflow.com/questions/1001540/how-to-write-a-utf-8-file-with-java
			CharsetEncoder encoder = Charset.forName(charset).newEncoder();
			encoder.onMalformedInput(CodingErrorAction.REPLACE);
			encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
			
			logNewFile.debug("creating file"+(append?" [append]":"")+": "+fname);
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
				if(doWrite!=null && doWrite) {
					w.write('\ufeff');
				}
			}
			/*else if("ISO-8859-1".equalsIgnoreCase(charset)) {
				//do nothing
			}*/
			else {
				if(doWrite!=null && doWrite) {
					log.warn("unknown BOM for charset '"+charset+"'");
				}
			}
		} catch (IOException e) {
			log.warn("error writing BOM [charset="+charset+"]: "+e);
		}
	}
	
	static Pattern colMatch = Pattern.compile("\\[col:(.+?)\\]");
	@Deprecated
	static Pattern colMatchOld = Pattern.compile("\\$\\{col:(.+?)\\}");
	
	static List<String> getPartitionCols(final String partitionByPattern) {
		List<String> rets = new ArrayList<String>();
		Matcher m = colMatch.matcher(partitionByPattern);
		while(m.find()) {
			rets.add(m.group(1));
		}

		//TODO: remove col match '${col:(.+?)}'
		m = colMatchOld.matcher(partitionByPattern);
		while(m.find()) {
			rets.add(m.group(1));
		}
		return rets;
	}
	
	static String getPartitionByStr(String partitionByStr, ResultSet rs, List<String> cols) throws SQLException {
		//XXX: numberformatter (leading 0s) for partitionId?
		//XXXxx: add dataformatter? useful for partitioning by year, year-month, ...
		//XXX: per-column dateFormatter?
		for(String c: cols) {
			String replacement = null;
			try {
				Object o = rs.getObject(c);
				if(o instanceof Date) {
					replacement = partitionByDateFormatter.format((Date)o);
				}
				else {
					replacement = String.valueOf(o); //rs.getString(c);
				}
			}
			catch(SQLException e) {
				log.warn("getPartitionByStr(): column '"+c+"' not found in result set");
				throw e;
			}
			//if(replacement==null) { replacement = ""; }
			replacement = Matcher.quoteReplacement(replacement);
			partitionByStr = partitionByStr.replaceAll("\\$\\{col:"+c+"\\}", replacement); //XXX: remove deprecated pattern style
			partitionByStr = partitionByStr.replaceAll("\\[col:"+c+"\\]", replacement);
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
		
	static List<DumpSyntax> getSyntaxList(Properties prop, String dumpSyntaxesProperty) {
		String syntaxes = prop.getProperty(dumpSyntaxesProperty);
		if(syntaxes==null) {
			return null;
		}
		
		String[] syntaxArr = syntaxes.split(",");
		return getSyntaxList(prop, syntaxArr);
	}
	
	public static List<DumpSyntax> getSyntaxList(Properties prop, String[] dumpSyntaxes) {
		List<DumpSyntax> syntaxList = new ArrayList<DumpSyntax>();
		for(String syntax: dumpSyntaxes) {
			boolean syntaxAdded = false;
			for(Class<? extends DumpSyntax> dsc: DumpSyntaxRegistry.getSyntaxes()) {
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
		
		return syntaxList;
	}
	
	public static List<String> getSyntaxListNames(List<DumpSyntax> syntaxList) {
		List<String> names = new ArrayList<String>();
		for(DumpSyntax ds: syntaxList) {
			names.add(ds.getSyntaxId());
		}
		return names;
	}
	
	static long getTableRowLimit(Properties prop, String tableOrQueryName) {
		Long globalRowLimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		Long tablerowlimit = Utils.getPropLong(prop, DATADUMP_PROP_PREFIX+tableOrQueryName+".rowlimit");
		return tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;
	}
	
}
