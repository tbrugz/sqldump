package tbrugz.sqldump;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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
	static final String PROP_DATADUMP_FILEPATTERN = "sqldump.datadump.filepattern";
	//static final String PROP_DATADUMP_INSERTINTO = "sqldump.datadump.useinsertintosyntax";
	static final String PROP_DATADUMP_SYNTAXES = "sqldump.datadump.dumpsyntaxes";
	static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.charset";
	static final String PROP_DATADUMP_ROWLIMIT = "sqldump.datadump.rowlimit";
	static final String PROP_DATADUMP_TABLES = "sqldump.datadump.tables";
	static final String PROP_DATADUMP_DATEFORMAT = "sqldump.datadump.dateformat";
	static final String PROP_DATADUMP_ORDERBYPK = "sqldump.datadump.orderbypk";
	
	static final String PROP_DATADUMP_INSERTINTO_FILEPATTERN = "sqldump.datadump.insertinto.filepattern";
	static final String PROP_DATADUMP_CSV_FILEPATTERN = "sqldump.datadump.csv.filepattern";
	static final String PROP_DATADUMP_XML_FILEPATTERN = "sqldump.datadump.xml.filepattern";
	static final String PROP_DATADUMP_JSON_FILEPATTERN = "sqldump.datadump.json.filepattern";
	
	//datadump syntaxes
	static final String SYNTAX_INSERTINTO = "insertinto";
	static final String SYNTAX_CSV = "csv";
	static final String SYNTAX_XML = "xml";
	static final String SYNTAX_JSON = "json"; 

	//defaults
	static final String CHARSET_DEFAULT = "UTF-8";
	
	static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	static final String FILENAME_PATTERN_SYNTAXFILEEXT = "\\$\\{syntaxfileext\\}";
	
	static Logger log = Logger.getLogger(DataDump.class);
	
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

	Set<String> filesOpened = new HashSet<String>();
	
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
			
			runQuery(conn, sql, null, prop, tableName, charset, 
					rowlimit, 
					syntaxList
					);
		}
		
		if(tables4dump.size()>0) {
			log.warn("tables selected for dump but not found: "+Utils.join(tables4dump, ", "));
		}
	}
	
	public void runQuery(Connection conn, String sql, List<String> params, Properties prop, String tableName, String charset, 
			long rowlimit,
			List<DumpSyntax> syntaxList
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
			
			String defaultFilename = prop.getProperty(PROP_DATADUMP_FILEPATTERN);

			List<Writer> writerList = new ArrayList<Writer>();
			List<Boolean> doSyntaxDumpList = new ArrayList<Boolean>();
			
			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				ds.initDump(tableName, md);
				doSyntaxDumpList.add(false);
				writerList.add(null);
				
				String filename = prop.getProperty("sqldump.datadump."+ds.getSyntaxId()+".filepattern", defaultFilename);
				if(filename==null) {
					log.warn("no output file defined for syntax '"+ds.getSyntaxId()+"'");
				}
				else {
					filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, tableName);
					filename = filename.replaceAll(FILENAME_PATTERN_SYNTAXFILEEXT, ds.getDefaultFileExtension());
					boolean alreadyOpened = filesOpened.contains(filename);
					if(!alreadyOpened) { filesOpened.add(filename); }
					
					doSyntaxDumpList.set(i, true);
					writerList.set(i, new OutputStreamWriter(new FileOutputStream(filename, alreadyOpened), charset));
					ds.dumpHeader(writerList.get(i));
				}
			}
			
			int count = 0;
			do {
				for(int i=0;i<syntaxList.size();i++) {
					DumpSyntax ds = syntaxList.get(i);
					if(doSyntaxDumpList.get(i)) {
						ds.dumpRow(rs, count, writerList.get(i));
					}
				}
				count++;
				if(rowlimit<=count) { break; }
			}
			while(rs.next());
			log.info("dumped "+count+" rows from table: "+tableName);

			for(int i=0;i<syntaxList.size();i++) {
				DumpSyntax ds = syntaxList.get(i);
				if(doSyntaxDumpList.get(i)) {
					ds.dumpFooter(writerList.get(i));
					writerList.get(i).close();
				}
			}
			
			rs.close();
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
