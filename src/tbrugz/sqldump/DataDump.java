package tbrugz.sqldump;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

/*
 * TODO: prop for selecting which tables to dump data from
 * TODOne: limit number of rows to dump
 * TODOne: where clause for data dump 
 * TODO: column values escaping
 * TODOne: 'insert into' datadump syntax:
 *   sqldump.datadump.useinsertintosyntax=false
 *   sqldump.datadump.useinsertintosyntax.withcolumnnames=true
 */
public class DataDump {

	//generic props
	static final String PROP_DATADUMP_FILEPATTERN = "sqldump.datadump.filepattern";
	static final String PROP_DATADUMP_INSERTINTO = "sqldump.datadump.useinsertintosyntax";
	static final String PROP_DATADUMP_CHARSET = "sqldump.datadump.charset";
	static final String PROP_DATADUMP_ROWLIMIT = "sqldump.datadump.rowlimit";

	//'insert into' props
	static final String PROP_DATADUMP_INSERTINTO_WITHCOLNAMES = "sqldump.datadump.useinsertintosyntax.withcolumnnames";

	//'raw dump' props
	static final String PROP_DATADUMP_TABLENAMEHEADER = "sqldump.datadump.tablenameheader";
	static final String PROP_DATADUMP_COLUMNNAMESHEADER = "sqldump.datadump.columnnamesheader";
	static final String PROP_DATADUMP_RECORDDELIMITER = "sqldump.datadump.recorddelimiter";
	static final String PROP_DATADUMP_COLUMNDELIMITER = "sqldump.datadump.columndelimiter";

	//defaults
	static final String DELIM_RECORD_DEFAULT = "\n";
	static final String DELIM_COLUMN_DEFAULT = ";";
	static final String CHARSET_DEFAULT = "UTF-8";
	
	static final String FILENAME_PATTERN_TABLENAME = "\\$\\{tablename\\}";
	
	static Logger log = Logger.getLogger(SQLDump.class);
	
	void dumpData(Connection conn, List<String> tableNamesForDataDump, Properties prop) throws Exception {
		log.info("data dumping...");
		Long globalRowlimit = Utils.getPropLong(prop, DataDump.PROP_DATADUMP_ROWLIMIT);
		
		boolean doInsertIntoDump = "true".equals(prop.getProperty(PROP_DATADUMP_INSERTINTO, "false"));
		if(doInsertIntoDump) {
			dumpDataInsertIntoSyntax(conn, tableNamesForDataDump, prop, globalRowlimit);
		}
		else {
			dumpDataRawSyntax(conn, tableNamesForDataDump, prop, globalRowlimit);
		}
	}

	Set<String> filesOpened = new HashSet<String>();
	
	void dumpDataRawSyntax(Connection conn, List<String> tableNamesForDataDump, Properties prop, Long globalRowLimit) throws Exception {
		String recordDelimiter = prop.getProperty(PROP_DATADUMP_RECORDDELIMITER, DELIM_RECORD_DEFAULT);
		String columnDelimiter = prop.getProperty(PROP_DATADUMP_COLUMNDELIMITER, DELIM_COLUMN_DEFAULT);
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean doTableNameHeaderDump = "true".equals(prop.getProperty(PROP_DATADUMP_TABLENAMEHEADER, "false"));
		boolean doColumnNamesHeaderDump = "true".equals(prop.getProperty(PROP_DATADUMP_COLUMNNAMESHEADER, "false"));
		
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
		
		for(String table: tableNamesForDataDump) {
			String filename = prop.getProperty(PROP_DATADUMP_FILEPATTERN);
			filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, table);

			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.datadump."+table+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;
			
			boolean alreadyOpened = filesOpened.contains(filename);
			if(!alreadyOpened) { filesOpened.add(filename); }
			//Writer fos = new PrintWriter(filename, charset);
			//if already opened, append; if not, create
			Writer fos = new OutputStreamWriter(new FileOutputStream(filename, alreadyOpened), charset); 
			
			String whereClause = prop.getProperty("sqldump.datadump."+table+".where");

			log.info("dumping data from table: "+table);
			Statement st = conn.createStatement();
			String sql = "select * from \""+table+"\" "
					+ (whereClause!=null?" where "+whereClause:"");
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();

			//headers
			if(doTableNameHeaderDump) {
				out("[table "+table+"]", fos, recordDelimiter);
			}
			if(doColumnNamesHeaderDump) {
				StringBuffer sb = new StringBuffer();
				for(int i=0;i<numCol;i++) {
					sb.append(md.getColumnName(i+1)+columnDelimiter);
				}
				out(sb.toString(), fos, recordDelimiter);
			}
			
			int count = 0;
			while(rs.next()) {
				out(SQLUtils.getRowFromRS(rs, numCol, table, columnDelimiter), fos, recordDelimiter);
				count++;
				if(rowlimit<=count) { break; }
			}
			log.info("dumped "+count+" rows from table: "+table);

			rs.close();
			
			fos.close();
		}
		
	}

	void dumpDataInsertIntoSyntax(Connection conn, List<String> tableNamesForDataDump, Properties prop, Long globalRowLimit) throws Exception {
		String charset = prop.getProperty(PROP_DATADUMP_CHARSET, CHARSET_DEFAULT);
		boolean doColumnNamesDump = "true".equals(prop.getProperty(PROP_DATADUMP_INSERTINTO_WITHCOLNAMES, "true"));
		
		for(String table: tableNamesForDataDump) {
			String filename = prop.getProperty(PROP_DATADUMP_FILEPATTERN);
			filename = filename.replaceAll(FILENAME_PATTERN_TABLENAME, table);
			Long tablerowlimit = Utils.getPropLong(prop, "sqldump.datadump."+table+".rowlimit");
			long rowlimit = tablerowlimit!=null?tablerowlimit:globalRowLimit!=null?globalRowLimit:Long.MAX_VALUE;

			String whereClause = prop.getProperty("sqldump.datadump."+table+".where");

			log.debug("dumping data/inserts from table: "+table);
			Statement st = conn.createStatement();
			String sql = "select * from \""+table+"\""
					+ (whereClause!=null?" where "+whereClause:"");
			log.info("sql: "+sql);
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData md = rs.getMetaData();
			int numCol = md.getColumnCount();

			boolean hasData = rs.next();
			//so empty tables do not create empty dump files
			if(!hasData) continue;

			boolean alreadyOpened = filesOpened.contains(filename);
			if(!alreadyOpened) { filesOpened.add(filename); }
			//Writer fos = new PrintWriter(filename, charset);
			//if already opened, append; if not, create
			Writer fos = new OutputStreamWriter(new FileOutputStream(filename, alreadyOpened), charset); 
			
			//headers
			String colNames = "";
			List<String> lsColNames = new ArrayList<String>();
			List<Class> lsColTypes = new ArrayList<Class>();
			for(int i=0;i<numCol;i++) {
				lsColNames.add(md.getColumnName(i+1));
			}
			for(int i=0;i<numCol;i++) {
				lsColTypes.add(SQLUtils.getClassFromSqlType(md.getColumnType(i+1)));
			}
			if(doColumnNamesDump) {
				colNames = "("+Utils.join(lsColNames, ", ")+") ";
			}
			
			log.debug("colnames: "+lsColNames);
			log.debug("coltypes: "+lsColTypes);
			
			//TODOne: integet/float vals without quotes?
			int count = 0;
			do {
				List vals = SQLUtils.getRowObjectListFromRS(rs, lsColTypes, numCol);
				out("insert into "+table+" "+
					colNames+"values ("+
					Utils.join4sql(vals, ", ")+");", fos, "\n");
					//Utils.join4sql(vals, ", ", "'", true)+");", fos, "\n");
				count++;
				if(rowlimit<=count) { break; }
			}
			while(rs.next());
			log.info("dumped "+count+" rows from table: "+table);
			
			rs.close();
			fos.close();
		}
		
	}

	void out(String s, Writer pw, String recordDelimiter) throws IOException {
		pw.write(s+recordDelimiter);
	}
	
}
